from __future__ import annotations

from pathlib import Path

import pytest
from airflow.sdk import Asset

from airflow_provider_opensysml.assets import asset_name_for, sysml_model_asset
from airflow_provider_opensysml.triggers.model import SysMLModelChangedTrigger


def test_asset_over_model_file(tmp_path: Path):
    model = tmp_path / "rover v2.sysml"
    model.write_text("package Rover;")
    asset = sysml_model_asset(model, poll_interval=5, group="models")
    assert isinstance(asset, Asset)
    assert asset.name == "rover_v2.sysml"
    assert asset.uri == model.absolute().as_uri()
    assert asset.group == "models"
    (watcher,) = asset.watchers
    assert watcher.name == "rover_v2.sysml_changed"
    assert isinstance(watcher.trigger, SysMLModelChangedTrigger)
    assert watcher.trigger.path == str(model.absolute())
    assert watcher.trigger.poll_interval == 5


def test_explicit_names_and_patterns(tmp_path: Path):
    asset = sysml_model_asset(tmp_path, name="rover", watcher_name="rover_head", patterns=["*.sysml"])
    (watcher,) = asset.watchers
    assert asset.name == "rover" and watcher.name == "rover_head"
    assert watcher.trigger.patterns == ["*.sysml"]


def test_relative_path_is_made_absolute(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.chdir(tmp_path)
    asset = sysml_model_asset("models/rover.sysml")
    assert asset.uri == (tmp_path / "models" / "rover.sysml").as_uri()


def test_default_name():
    assert asset_name_for("/x/rover.sysml") == "rover.sysml"
    assert asset_name_for("/x/my model (v2).sysml") == "my_model_v2_.sysml"
    assert asset_name_for("/x/") == "x"
