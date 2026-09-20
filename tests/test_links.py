from unittest.mock import MagicMock

from sparkforensics_operator.links import ReportLink


def test_get_link_returns_the_xcom_return_value_on_airflow_2(monkeypatch):
    ti_key = MagicMock()
    fake_xcom_module = MagicMock()
    fake_xcom_module.XCom.get_value.return_value = "s3://reports/app-1.json"
    # Simulate Airflow 2.x: no XComModel on the xcom module.
    del fake_xcom_module.XComModel
    monkeypatch.setattr("sparkforensics_operator.links._xcom_module", lambda: fake_xcom_module)

    link = ReportLink()
    result = link.get_link(MagicMock(), ti_key=ti_key)

    assert result == "s3://reports/app-1.json"
    fake_xcom_module.XCom.get_value.assert_called_once_with(ti_key=ti_key, key="return_value")


def test_get_link_returns_the_xcom_return_value_on_airflow_3(monkeypatch):
    ti_key = MagicMock(run_id="run-1", dag_id="dag-1", task_id="task-1", map_index=-1)
    fake_xcom_module = MagicMock()
    fake_row = MagicMock()
    fake_xcom_module.XComModel.get_many.return_value.first.return_value = fake_row
    fake_xcom_module.XComModel.deserialize_value.return_value = "s3://reports/app-1.json"
    monkeypatch.setattr("sparkforensics_operator.links._xcom_module", lambda: fake_xcom_module)

    link = ReportLink()
    result = link.get_link(MagicMock(), ti_key=ti_key)

    assert result == "s3://reports/app-1.json"
    fake_xcom_module.XComModel.get_many.assert_called_once_with(
        key="return_value", run_id="run-1", dag_ids="dag-1", task_ids="task-1", map_indexes=-1,
    )


def test_get_link_returns_empty_string_on_any_failure(monkeypatch):
    def boom():
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("sparkforensics_operator.links._xcom_module", boom)

    link = ReportLink()
    assert link.get_link(MagicMock(), ti_key=MagicMock()) == ""


def test_get_link_returns_empty_string_when_no_xcom_value_exists(monkeypatch):
    fake_xcom_module = MagicMock()
    fake_xcom_module.XComModel.get_many.return_value.first.return_value = None
    monkeypatch.setattr("sparkforensics_operator.links._xcom_module", lambda: fake_xcom_module)

    link = ReportLink()
    assert link.get_link(MagicMock(), ti_key=MagicMock()) == ""
