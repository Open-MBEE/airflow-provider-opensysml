"""SysML models as Airflow assets."""

from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from airflow.sdk import Asset, AssetWatcher

from airflow_provider_opensysml.triggers.model import SysMLModelChangedTrigger

_NAME_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]+")


def asset_name_for(path: str | Path) -> str:
    """A default asset name for a model path: its file name with unsafe characters replaced."""
    name = _NAME_UNSAFE.sub("_", Path(path).name).strip("_")
    return name or "sysml_model"


def sysml_model_asset(
    path: str | Path,
    *,
    name: str | None = None,
    patterns: Sequence[str] | None = None,
    poll_interval: float = 30.0,
    settle_interval: float = 2.0,
    watcher_name: str | None = None,
    **asset_kwargs: Any,
) -> Asset:
    """An :class:`~airflow.sdk.Asset` for the SysML model at ``path``, watched for changes.

    The asset's URI is the ``file://`` URI of the absolute path; its one watcher
    is a :class:`SysMLModelChangedTrigger` over the same path, so a DAG with
    ``schedule=[sysml_model_asset(...)]`` runs each time the model changes. The
    path is resolved where the DAG file is parsed and must be visible to the
    triggerer under the same path.

    :param path: The model file or directory
    :param name: The asset name; defaults to the path's file name
    :param patterns: Glob patterns selecting files under a directory
    :param poll_interval: Seconds between the watcher's digests
    :param settle_interval: Seconds a change must hold still before it is reported
    :param watcher_name: The watcher's name; defaults to ``<name>_changed``
    :param asset_kwargs: Passed through to :class:`~airflow.sdk.Asset` (``group``, ``extra``, ...)
    """
    resolved = Path(path).expanduser().absolute()
    asset_name = name or asset_name_for(resolved)
    trigger = SysMLModelChangedTrigger(
        path=str(resolved),
        patterns=patterns,
        poll_interval=poll_interval,
        settle_interval=settle_interval,
    )
    watcher = AssetWatcher(name=watcher_name or f"{asset_name}_changed", trigger=trigger)
    return Asset(name=asset_name, uri=resolved.as_uri(), watchers=[watcher], **asset_kwargs)
