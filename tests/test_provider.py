from __future__ import annotations

import importlib
from importlib.metadata import entry_points

import pytest

from airflow_provider_opensysml import __version__, get_provider_info


def test_provider_info_names_importable_modules():
    info = get_provider_info()
    assert info["package-name"] == "airflow-provider-opensysml"
    assert info["versions"] == [__version__]
    for section in ("hooks", "operators", "triggers"):
        for entry in info[section]:
            for module in entry["python-modules"]:
                importlib.import_module(module)
    (conn,) = info["connection-types"]
    module, _, cls = conn["hook-class-name"].rpartition(".")
    hook = getattr(importlib.import_module(module), cls)
    assert hook.conn_type == conn["connection-type"] == "opensysml"


def test_entry_point_registered():
    eps = [ep for ep in entry_points(group="apache_airflow_provider") if ep.name == "provider_info"]
    ours = [ep for ep in eps if ep.value.startswith("airflow_provider_opensysml")]
    if not ours:
        pytest.skip("package not installed; the entry point is only visible after `pip install`")
    assert ours[0].load()() == get_provider_info()


def test_providers_manager_discovers_hook_and_trigger():
    pytest.importorskip("airflow.providers_manager")
    from airflow.providers_manager import ProvidersManager

    manager = ProvidersManager()
    if "airflow-provider-opensysml" not in manager.providers:
        pytest.skip("package not installed")
    assert "opensysml" in manager.hooks
    assert "airflow_provider_opensysml.triggers.model" in [t.trigger_class_name for t in manager.trigger]
