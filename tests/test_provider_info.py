import json
from importlib.metadata import distribution, version
from pathlib import Path

import airflow

from sparkforensics_operator.get_provider_info import get_provider_info
from sparkforensics_operator.links import ReportLink


def test_provider_info_validates_against_airflows_runtime_schema():
    import jsonschema

    schema = json.loads((Path(airflow.__file__).parent / "provider_info.schema.json").read_text())

    jsonschema.validate(get_provider_info(), schema)


def test_provider_info_names_the_installed_distribution_and_version():
    info = get_provider_info()

    # ProvidersManager refuses provider info whose package-name differs from
    # the distribution that declared the entry point.
    assert info["package-name"] == distribution("sparkforensics-operator").metadata["Name"]
    assert info["versions"] == [version("sparkforensics-operator")]


def test_every_listed_hook_and_link_module_imports():
    import importlib

    info = get_provider_info()
    for entry in info["hooks"] + info["operators"]:
        for module in entry["python-modules"]:
            importlib.import_module(module)
    [link] = info["extra-links"]
    module, _, name = link.rpartition(".")
    assert getattr(importlib.import_module(module), name) is ReportLink


def test_airflow_discovers_the_provider_and_registers_the_report_link():
    from airflow.providers_manager import ProvidersManager

    manager = ProvidersManager()

    assert "sparkforensics-operator" in manager.providers
    assert "sparkforensics_operator.links.ReportLink" in manager.extra_links_class_names


def test_the_distribution_is_classified_as_an_airflow_provider():
    classifiers = distribution("sparkforensics-operator").metadata.get_all("Classifier")

    assert "Framework :: Apache Airflow :: Provider" in classifiers
