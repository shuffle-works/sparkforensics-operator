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


def task_instance_scope(context: Any) -> str:
    """The name of this task instance's directory under the base directory:
    the same for every try of one (dag_id, task_id, run_id, map_index), and
    different for any other. The digest keeps names unique however long the
    ids are; the readable prefix is for whoever lists the directory."""
    ti = context["ti"]
    key = json.dumps([ti.dag_id, ti.task_id, ti.run_id, getattr(ti, "map_index", -1)])
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


# Stops every job under "$scope" that has not written its exit code yet,
# then removes "$scope". A pid is signalled only while its command line
# still names its own job directory (by its unique name: a synchronous
# job's script holds the path unexpanded): after a host reboot the recorded
# pid may belong to an unrelated process. The provider's wrapper makes the
# detached job a session leader under setsid, so its pid is also its
# session id: signalling the session reaches coreutils `timeout` and the CLI
# too, which a process group kill would miss, since `timeout` moves itself
# into a group of its own. Without pkill, or on a host without setsid, it
# falls back to the group and then the single process, as the provider's
# own kill does. A synchronous job's script forwards the signal itself
# (sync_analysis_command).
_SWEEP_SCOPE = """\
if [ -d "$scope" ]; then
  for pid_file in "$scope"/*/pid; do
    [ -f "$pid_file" ] || continue
    job_dir=${pid_file%/pid}
    [ -e "$job_dir/exit_code" ] && continue
    p=$(cat "$pid_file" 2>/dev/null || true)
    [ "$p" -gt 1 ] 2>/dev/null || continue
    args=$(tr '\\000' ' ' < "/proc/$p/cmdline" 2>/dev/null || ps -ww -o args= -p "$p" 2>/dev/null || true)
    case "$args" in
      *"${job_dir##*/}"*)
        pkill -TERM -s "$p" 2>/dev/null ||
          kill -TERM -"$p" 2>/dev/null || kill -TERM "$p" 2>/dev/null || true ;;
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


# The synchronous run as a trackable job: the CLI's stdout and stderr stay
# on the SSH channel, as before, but its pid is recorded under a job
# directory so on_kill can stop it with the same sweep as a detached job.
# Closing the channel alone would not: a non-PTY channel sends the remote
# process no signal. The script's own pid is the one recorded; on TERM it signals
# coreutils `timeout`'s own process group (the CLI and its children) and
# removes its directory. Exit 125, which `timeout` itself uses for its own
# failures, means the job directory could not be set up.
_SYNC_JOB_SCRIPT = """\
set -u
base={base}
scope="$base"/{scope}
job_dir="$scope"/{job_name}
mkdir -p -- "$scope" || exit 125
mkdir -m 700 -- "$job_dir" || exit 125
echo "$$" > "$job_dir/pid" || exit 125
child=
finish() {{ rm -rf -- "$job_dir"; rmdir -- "$scope" 2>/dev/null; }}
trap 'if [ -n "$child" ]; then kill -TERM -"$child" 2>/dev/null || kill -TERM "$child" 2>/dev/null; fi; finish; exit 143' TERM HUP INT
{cli} &
child=$!
wait "$child"
ec=$?
finish
exit "$ec"
"""


def sync_analysis_command(
    remote_base_dir: str | None,
    scope: str,
    job_name: str,
    analyze_bin: str,
    log_ref: EventLogRef,
    thresholds: dict,
    timeout: int,
) -> str:
    """The synchronous path's command: the same CLI invocation, writing the
    report to stdout, run as a job abandon_scope_command(scope) can stop."""
    cli = shlex.join(["timeout", str(timeout), *build_cli_args(analyze_bin, log_ref, thresholds)])
    script = _SYNC_JOB_SCRIPT.format(
        base=_base_dir_expr(remote_base_dir),
        scope=shlex.quote(scope),
        job_name=shlex.quote(job_name),
        cli=cli,
    )
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
