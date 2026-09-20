from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from sparkforensics_operator.hooks.log_source.tunnel import SSHTunneledLogSourceHook

pytest.importorskip("airflow.providers.ssh.hooks.ssh")


class _FakeTunnel:
    def __init__(self, local_bind_port):
        self.local_bind_port = local_bind_port
        self.exited = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.exited = True
        return False


def _hook(hook_factory, ssh_conn_id="onprem_ssh", remote_host="shs.internal", remote_port=18080):
    return SSHTunneledLogSourceHook(
        ssh_conn_id=ssh_conn_id, remote_host=remote_host, remote_port=remote_port,
        hook_factory=hook_factory,
    )


def test_fetch_builds_the_base_url_from_the_tunnels_local_bind_port_and_delegates():
    tunnel = _FakeTunnel(local_bind_port=54321)
    mock_ssh_hook = MagicMock()
    mock_ssh_hook.get_tunnel.return_value = tunnel
    inner_hook = MagicMock()
    inner_hook.fetch.return_value = "log-path"
    hook_factory = MagicMock(return_value=inner_hook)
    hook = _hook(hook_factory)

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook", return_value=mock_ssh_hook) as mock_cls:
        result = hook.fetch({"some": "context"})

    mock_cls.assert_called_once_with(ssh_conn_id="onprem_ssh")
    mock_ssh_hook.get_tunnel.assert_called_once_with(remote_port=18080, remote_host="shs.internal")
    hook_factory.assert_called_once_with("http://127.0.0.1:54321")
    inner_hook.fetch.assert_called_once_with({"some": "context"})
    assert result == "log-path"
    assert tunnel.exited is True


def test_cleanup_delegates_to_the_inner_hook_built_by_the_factory():
    tunnel = _FakeTunnel(local_bind_port=1234)
    mock_ssh_hook = MagicMock()
    mock_ssh_hook.get_tunnel.return_value = tunnel
    inner_hook = MagicMock()
    hook_factory = MagicMock(return_value=inner_hook)
    hook = _hook(hook_factory)

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook", return_value=mock_ssh_hook):
        hook.fetch({})
    hook.cleanup("log-path")

    inner_hook.cleanup.assert_called_once_with("log-path")


def test_cleanup_is_a_no_op_if_fetch_was_never_called():
    hook = _hook(hook_factory=MagicMock())

    hook.cleanup("some-path")  # must not raise


def test_fetch_wraps_an_ssh_connection_failure_in_an_airflowexception():
    hook = _hook(hook_factory=MagicMock())

    with patch(
        "airflow.providers.ssh.hooks.ssh.SSHHook", side_effect=OSError("no route to host"),
    ):
        with pytest.raises(AirflowException, match="onprem_ssh"):
            hook.fetch({})


def test_fetch_propagates_the_inner_hooks_own_airflowexception_unwrapped():
    tunnel = _FakeTunnel(local_bind_port=1234)
    mock_ssh_hook = MagicMock()
    mock_ssh_hook.get_tunnel.return_value = tunnel
    inner_hook = MagicMock()
    inner_hook.fetch.side_effect = AirflowException("Spark History Server log download failed (500)")
    hook_factory = MagicMock(return_value=inner_hook)
    hook = _hook(hook_factory)

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook", return_value=mock_ssh_hook):
        with pytest.raises(AirflowException) as exc_info:
            hook.fetch({})
        assert str(exc_info.value) == "Spark History Server log download failed (500)"


def test_fetch_propagates_the_inner_hooks_own_non_airflow_exception_unwrapped():
    tunnel = _FakeTunnel(local_bind_port=1234)
    mock_ssh_hook = MagicMock()
    mock_ssh_hook.get_tunnel.return_value = tunnel
    inner_hook = MagicMock()
    inner_hook.fetch.side_effect = ValueError("connection refused")
    hook_factory = MagicMock(return_value=inner_hook)
    hook = _hook(hook_factory)

    with patch("airflow.providers.ssh.hooks.ssh.SSHHook", return_value=mock_ssh_hook):
        with pytest.raises(ValueError) as exc_info:
            hook.fetch({})
        assert str(exc_info.value) == "connection refused"
