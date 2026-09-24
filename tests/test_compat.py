from sparkforensics_operator import _compat


def test_base_operator_is_a_real_airflow_base_operator_class():
    assert isinstance(_compat.BaseOperator, type)
    assert _compat.BaseOperator.__name__ == "BaseOperator"


def test_base_operator_link_is_a_real_airflow_base_operator_link_class():
    assert isinstance(_compat.BaseOperatorLink, type)
    assert _compat.BaseOperatorLink.__name__ == "BaseOperatorLink"


def test_base_operator_link_get_link_signature_takes_operator_and_ti_key():
    import inspect

    sig = inspect.signature(_compat.BaseOperatorLink.get_link)
    assert list(sig.parameters) == ["self", "operator", "ti_key"]
    assert sig.parameters["ti_key"].kind == inspect.Parameter.KEYWORD_ONLY


def test_airflow_v3_plus_matches_the_installed_airflow_major_version():
    import airflow

    assert _compat.AIRFLOW_V3_PLUS is (int(airflow.__version__.split(".")[0]) >= 3)
