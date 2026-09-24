from __future__ import annotations

from typing import Callable

from airflow.exceptions import AirflowException

from sparkforensics_operator.log_ref import EventLogRef, LocalEventLog

from .base import LogSourceHook


class SSHTunneledLogSourceHook(LogSourceHook):
    """Tunnels network access to any HTTP-based LogSourceHook through the
    same SSH connection already configured for SSHOperator, for consumers
    whose Spark History Server sits on a private on-prem network the cloud
    Airflow worker can't route to directly. A generic wrapper via
    composition, not a History-Server-specific hook: hook_factory builds
    the actual LogSourceHook (e.g. HistoryServerLogSourceHook) once the
    tunnel's local port is known, so the same wrapper works for any future
    HTTP-based hook with no new wrapper code. The wrapped hook must fetch
    the log to the worker (return a LocalEventLog): the tunnel closes when
    resolve() returns, so a reference that still points through it, such
    as a HistoryServerApp, would be dead by the time it is analyzed."""

    def __init__(
        self,
        ssh_conn_id: str,
        remote_host: str,
        remote_port: int,
        hook_factory: Callable[[str], LogSourceHook],
    ):
        super().__init__()
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.hook_factory = hook_factory
        self._inner: LogSourceHook | None = None

    def resolve(self, context: dict) -> LocalEventLog:
        from airflow.providers.ssh.hooks.ssh import SSHHook

        tunnel_up = False
        fetch_succeeded = False
        try:
            ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
            with ssh_hook.get_tunnel(
                remote_port=self.remote_port, remote_host=self.remote_host
            ) as tunnel:
                tunnel_up = True
                base_url = f"http://127.0.0.1:{tunnel.local_bind_port}"
                self._inner = self.hook_factory(base_url)
                result = self._inner.resolve(context)
                fetch_succeeded = True
            if not isinstance(result, LocalEventLog):
                raise AirflowException(
                    f"SSHTunneledLogSourceHook's hook_factory built a hook that returned a "
                    f"{type(result).__name__}, not a LocalEventLog: the SSH tunnel is closed "
                    "once resolve() returns, so the log must be fetched to the worker while "
                    "it is open. To analyze a History Server run without downloading it, "
                    "use HistoryServerAppLogSourceHook with SSHAnalyzeHook instead."
                )
            return result
        except AirflowException:
            raise
        except Exception as e:
            if fetch_succeeded:
                # The fetch itself already succeeded; this failure happened
                # while the `with` block tore the tunnel back down. Label it
                # as a teardown failure instead of silently discarding the
                # fetched result and re-raising as if the failure came from
                # hook_factory or the wrapped hook's own resolve().
                raise AirflowException(
                    f"SSH tunnel teardown failed after a successful fetch "
                    f"(ssh_conn_id={self.ssh_conn_id!r}): {e}"
                ) from e
            if tunnel_up:
                # The tunnel itself came up fine; this failure happened in
                # hook_factory or the wrapped hook's own resolve(), not in
                # tunnel setup. Let it propagate as its own type/message
                # instead of mislabeling it as a tunnel-setup failure.
                raise
            raise AirflowException(
                f"SSH tunnel setup failed (ssh_conn_id={self.ssh_conn_id!r}, "
                f"remote={self.remote_host}:{self.remote_port}): {e}"
            ) from e

    def cleanup(self, log_ref: EventLogRef) -> None:
        if self._inner is not None:
            self._inner.cleanup(log_ref)
