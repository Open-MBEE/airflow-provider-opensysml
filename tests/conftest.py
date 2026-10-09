from __future__ import annotations

import shutil
from pathlib import Path

import pytest

MODEL = Path(__file__).parent.parent / "example_dags" / "lander_verification" / "lander.sysml"


@pytest.fixture
def lander(tmp_path: Path) -> Path:
    """A writable copy of the example lander model."""
    copy = tmp_path / "lander.sysml"
    shutil.copy(MODEL, copy)
    return copy


@pytest.fixture
def lander_source() -> str:
    return MODEL.read_text()
