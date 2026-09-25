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


def test_importing_the_package_emits_no_deprecation_warning_from_its_own_modules():
    # A fresh interpreter, so every module is imported (and warns) here, not
    # served from sys.modules. Airflow reports a moved import with a warning
    # attributed to the importing module, i.e. one of ours.
    import subprocess
    import sys

    # airflow is imported before recording starts: importing it turns on
    # logging.captureWarnings, which would replace the recorder.
    script = """
import pkgutil, warnings
import airflow
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    import sparkforensics_operator
    for mod in pkgutil.walk_packages(sparkforensics_operator.__path__, "sparkforensics_operator."):
        __import__(mod.name)
for w in caught:
    if "sparkforensics_operator" in (w.filename or "") and issubclass(w.category, (DeprecationWarning, PendingDeprecationWarning, FutureWarning)):
        print(f"OUR-DEPRECATION {w.filename}:{w.lineno}: {w.category.__name__}: {w.message}")
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=120)

    assert result.returncode == 0, result.stderr
    # Airflow logs its own config deprecations to stdout too; only ours count.
    assert [line for line in result.stdout.splitlines() if line.startswith("OUR-DEPRECATION")] == []
