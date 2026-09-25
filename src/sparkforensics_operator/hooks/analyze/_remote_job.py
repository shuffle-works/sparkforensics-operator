"""
Shell commands for running sparkforensics-analyze as a detached job on a
POSIX SSH host, for SSHAnalyzeHook's deferrable mode. The job itself is the
SSH provider's (apache-airflow-providers-ssh >= 6.0.1): its RemoteJobPaths
layout, its wrapper that starts the command detached under setsid and writes
the exit code to a file, its cleanup command, and SSHRemoteJobTrigger to
wait on it. This module adds what the provider has no notion of: one
directory per task instance, so a new try can stop and remove whatever an
earlier try left behind.

Every value interpolated into a command goes through shlex.quote, and every
command runs under an explicit shell (`sh -c` or `bash -c`), so neither a
rendered path nor the SSH user's login shell changes what runs.
"""
from __future__ import annotations

import hashlib
import json
import re
import shlex
from pathlib import PurePosixPath
from typing import Any

from sparkforensics_operator._compat import AIRFLOW_V3_PLUS, conf
from sparkforensics_operator.log_ref import EventLogRef

from ._cli import build_cli_args

REPORT_FILE_NAME = "report.json"
STDERR_FILE_NAME = "stderr.log"

_UNSAFE_NAME_CHARS = re.compile(r"[^A-Za-z0-9]")
# The provider's wrapper splices the job's paths into a double-quoted string
# inside the script it runs, where these characters would be evaluated
# again: $ and ` expand, " and \ end or escape the quoting. Job ids and scope
# names never contain them, so only the base directory needs the check.
_UNSAFE_PATH_CHARS = re.compile(r'[$`"\\\x00-\x1f\x7f]')


def validate_remote_base_dir(path: str, what: str = "remote_base_dir") -> None:
    if not path.startswith("/"):
        raise ValueError(f"{what} must be an absolute path, got {path!r}")
    if _UNSAFE_PATH_CHARS.search(path):
        raise ValueError(
            f"{what} cannot contain $, `, \", \\ or control characters, got {path!r}"
        )
    if ".." in PurePosixPath(path).parts:
        raise ValueError(f"{what} cannot contain '..', got {path!r}")


def airflow_deployment() -> str:
    """This Airflow deployment's own URL ([api] base_url on Airflow 3,
    [webserver] base_url on Airflow 2). Two deployments running the same
    DAGs produce the same dag, task and run ids, e.g. for scheduled runs,
    and nothing else a worker can see tells them apart; their URLs differ."""
    section = "api" if AIRFLOW_V3_PLUS else "webserver"
    return (conf.get(section, "base_url", fallback="") or "").rstrip("/")


def task_instance_scope(context: Any) -> str:
    """The name of this task instance's directory under the base directory:
    the same for every try of one (dag_id, task_id, run_id, map_index) of
    this Airflow deployment, and different for any other, including the
    same task instance of another deployment sharing the SSH user. The
    digest keeps names unique however long the ids are; the readable prefix
    is for whoever lists the directory."""
    ti = context["ti"]
    key = json.dumps(
        [airflow_deployment(), ti.dag_id, ti.task_id, ti.run_id, getattr(ti, "map_index", -1)]
    )
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    dag = _UNSAFE_NAME_CHARS.sub("_", ti.dag_id)[:40]
    task = _UNSAFE_NAME_CHARS.sub("_", ti.task_id)[:40]
    return f"sf_{dag}_{task}_{digest}"


def _base_dir_expr(remote_base_dir: str | None) -> str:
    # The default is under the SSH user's home, not a shared /tmp: another
    # local user can't pre-create, read or tamper with a job directory there.
    if remote_base_dir is None:
        return '"${HOME:?HOME is not set on the SSH host}/.sparkforensics/jobs"'
    return shlex.quote(remote_base_dir)


# Defines stop_session, which sends SIGTERM to every process of session
# $1. pkill (procps) is missing on some hosts, e.g. slim container images,
# so without it the session's processes are looked up in /proc: the fourth
# field after the parenthesised command name in /proc/<pid>/stat is the
# session id. Only on a host with neither does it fall back to the process
# group and then the single process, as the provider's own kill does.
_STOP_SESSION = """\
stop_session() {
  pkill -TERM -s "$1" 2>/dev/null && return 0
  sid=$1 signalled=
  for stat_file in /proc/[0-9]*/stat; do
    IFS= read -r stat 2>/dev/null < "$stat_file" || continue
    set -- ${stat##*) }
    [ "${4:-}" = "$sid" ] || continue
    q=${stat_file#/proc/}
    kill -TERM "${q%/stat}" 2>/dev/null && signalled=1
  done
  [ -n "$signalled" ] ||
    kill -TERM -"$sid" 2>/dev/null || kill -TERM "$sid" 2>/dev/null || true
}
"""

# Stops every job under "$scope" that has not written its exit code yet,
# then removes "$scope". A pid is signalled only while its command line
# still names its own job directory (by its unique name): after a host
# reboot the recorded pid may belong to an unrelated process. The
# provider's wrapper makes the job a session leader under setsid, so its
# pid is also its session id: signalling the session reaches coreutils
# `timeout` and the CLI too, which a process group kill would miss, since
# `timeout` moves itself into a group of its own.
_SWEEP_SCOPE = _STOP_SESSION + """\
if [ -d "$scope" ]; then
  for pid_file in "$scope"/*/pid; do
    [ -f "$pid_file" ] || continue
    job_dir=${pid_file%/pid}
    [ -e "$job_dir/exit_code" ] && continue
    p=$(cat "$pid_file" 2>/dev/null || true)
    [ "$p" -gt 1 ] 2>/dev/null || continue
    args=$(tr '\\000' ' ' < "/proc/$p/cmdline" 2>/dev/null || ps -ww -o args= -p "$p" 2>/dev/null || true)
    case "$args" in
      *"${job_dir##*/}"*) stop_session "$p" ;;
    esac
  done
  rm -rf -- "$scope"
fi
"""


def prepare_scope_command(remote_base_dir: str | None, scope: str) -> str:
    """Sweeps the task instance's directory, recreates it empty and
    owner-only, and prints its absolute path."""
    script = (
        "set -eu\n"
        f"base={_base_dir_expr(remote_base_dir)}\n"
        f'scope="$base"/{shlex.quote(scope)}\n'
        f"{_SWEEP_SCOPE}"
        'mkdir -p -- "$base"\n'
        'mkdir -m 700 -- "$scope"\n'
        "printf '%s\\n' \"$scope\"\n"
    )
    return f"sh -c {shlex.quote(script)}"


def abandon_scope_command(remote_base_dir: str | None, scope: str) -> str:
    """Stops and removes whatever jobs the task instance's directory holds."""
    script = (
        "set -eu\n"
        f"base={_base_dir_expr(remote_base_dir)}\n"
        f'scope="$base"/{shlex.quote(scope)}\n'
        f"{_SWEEP_SCOPE}"
    )
    return f"sh -c {shlex.quote(script)}"


def abandon_scope_dir_command(scope_dir: str) -> str:
    """abandon_scope_command for an already-resolved directory path."""
    script = f"set -eu\nscope={shlex.quote(scope_dir)}\n{_SWEEP_SCOPE}"
    return f"sh -c {shlex.quote(script)}"


def analysis_command(
    analyze_bin: str,
    log_ref: EventLogRef,
    thresholds: dict,
    timeout: int,
    report_file: str,
    stderr_file: str,
) -> str:
    """The command the job runs: the same CLI invocation as the synchronous
    path, under the same coreutils `timeout`, writing the report to
    report_file instead of stdout. stderr, where the threshold lines are,
    goes to its own file: the provider's wrapper merges stdout and stderr
    into its log file, and the report is never parsed from that log."""
    cli = shlex.join(
        [
            "timeout",
            str(timeout),
            *build_cli_args(analyze_bin, log_ref, thresholds, out_path=PurePosixPath(report_file)),
        ]
    )
    return f"{{ {cli} 2>{shlex.quote(stderr_file)}; }}"


SYNC_PID_MARKER = "sparkforensics-pid:"

# The synchronous run: the CLI's stdout and stderr stay on the SSH channel,
# and nothing is written on the host. Closing a non-PTY channel sends the
# remote process no signal, so the launcher first prints its own pid on
# stdout, after SYNC_PID_MARKER, then execs the CLI under setsid, which
# keeps that pid and makes it the session leader. The launcher runs in the
# background of the outer shell so it is never a process group leader,
# which would make setsid fork and return at once. Without setsid the pid
# is coreutils `timeout`'s, which moves itself and the CLI into a group of
# its own.
_SYNC_LAUNCHER = f"""\
printf '{SYNC_PID_MARKER}%s\\n' "$$" || exit 1
if command -v setsid >/dev/null 2>&1; then exec setsid "$@"; fi
exec "$@"
"""

# Signals the session (or group, or process) of the synchronous run's pid,
# but only while that pid still runs the command line it was started with.
_STOP_SYNC_RUN = _STOP_SESSION + """\
args=$(tr '\\000' ' ' 2>/dev/null < "/proc/$p/cmdline" || ps -ww -o args= -p "$p" 2>/dev/null || true)
case "$args" in
  "$expected"|"$expected ") stop_session "$p" ;;
esac
"""


def sync_cli_argv(analyze_bin: str, log_ref: EventLogRef, thresholds: dict, timeout: int) -> list[str]:
    """The synchronous path's CLI invocation, writing the report to stdout."""
    return ["timeout", str(timeout), *build_cli_args(analyze_bin, log_ref, thresholds)]


def sync_analysis_command(argv: list[str]) -> str:
    """Runs argv on the host, first printing the pid stop_sync_command
    needs to stop it."""
    script = f"sh -c {shlex.quote(_SYNC_LAUNCHER)} sh {shlex.join(argv)} &\nwait \"$!\"\n"
    return f"sh -c {shlex.quote(script)}"


def stop_sync_command(pid: int, argv: list[str]) -> str:
    """Stops the run sync_analysis_command(argv) reported as pid."""
    script = f"p={int(pid)}\nexpected={shlex.quote(' '.join(argv))}\n{_STOP_SYNC_RUN}"
    return f"sh -c {shlex.quote(script)}"


def submit_command(command: str, job_id: str, scope_dir: str) -> tuple[str, dict]:
    """The provider's detached-job wrapper around command, run under bash
    (it uses `set -o pipefail`), and the job's paths."""
    from airflow.providers.ssh.utils.remote_job import RemoteJobPaths, build_posix_wrapper_command

    paths = RemoteJobPaths(job_id=job_id, remote_os="posix", base_dir=scope_dir)
    wrapper = build_posix_wrapper_command(command=command, paths=paths)
    job_paths = {
        "job_dir": paths.job_dir,
        "log_file": paths.log_file,
        "exit_code_file": paths.exit_code_file,
        "report_file": f"{paths.job_dir}/{REPORT_FILE_NAME}",
        "stderr_file": f"{paths.job_dir}/{STDERR_FILE_NAME}",
    }
    return f"bash -c {shlex.quote(wrapper)}", job_paths


def read_file_command(path: str) -> str:
    return f"cat -- {shlex.quote(path)}"


def cleanup_job_command(job_dir: str, scope_dir: str) -> str:
    """The provider's cleanup of one job directory (it refuses a path
    outside scope_dir), then the task instance's now-empty directory."""
    from airflow.providers.ssh.utils.remote_job import build_posix_cleanup_command

    script = (
        f"{build_posix_cleanup_command(job_dir, base_dir=scope_dir)}\n"
        f"rmdir -- {shlex.quote(scope_dir)} 2>/dev/null || true\n"
    )
    return f"sh -c {shlex.quote(script)}"
