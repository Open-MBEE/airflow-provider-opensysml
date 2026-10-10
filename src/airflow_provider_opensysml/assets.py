"""SysML models, and the requirements they satisfy, as Airflow assets."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote

from airflow.sdk import Asset, AssetWatcher

from airflow_provider_opensysml.hooks.opensysml import OpenSysMLHook
from airflow_provider_opensysml.questions import SysMLQuestion
from airflow_provider_opensysml.triggers.model import SysMLModelChangedTrigger
from airflow_provider_opensysml.triggers.requirement import SysMLRequirementSatisfiedTrigger

_NAME_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]+")


def asset_name_for(path: str | Path, element: str | None = None) -> str:
    """A default asset name: the model file name, then ``:<element>`` when given."""
    name = _NAME_UNSAFE.sub("_", Path(path).name).strip("_") or "sysml_model"
    if element:
        name = f"{name}:{_NAME_UNSAFE.sub('_', element).strip('_')}"
    return name


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


def sysml_requirement_asset(
    path: str | Path,
    element: str | None,
    *,
    kind: str = "requirement",
    name: str | None = None,
    subject: str | None = None,
    engine: str | None = None,
    question: str | None = None,
    arguments: Sequence[Any] | None = None,
    named_arguments: dict[str, Any] | None = None,
    schedule: str | None = None,
    opensysml_conn_id: str = OpenSysMLHook.default_conn_name,
    strict: bool = True,
    patterns: Sequence[str] | None = None,
    poll_interval: float = 30.0,
    settle_interval: float = 2.0,
    watcher_name: str | None = None,
    **asset_kwargs: Any,
) -> Asset:
    """An :class:`~airflow.sdk.Asset` updated each time the model at ``path`` comes to satisfy ``element``.

    The asset's URI is the model's ``file://`` URI with ``?satisfies=<element>``;
    its one watcher is a :class:`SysMLRequirementSatisfiedTrigger`
    asking the question of the model at each change of its files, so a DAG with
    ``schedule=[sysml_requirement_asset(...)]`` runs each time the answer turns
    from not holding to holding. ``kind``, ``subject``, ``engine``, ``question``,
    ``arguments``, ``named_arguments`` and ``schedule`` are the question's, as
    :class:`~airflow_provider_opensysml.questions.SysMLQuestion` takes them; the
    arguments of a ``case`` are fixed when the DAG file is parsed.

    :param path: The model file or directory
    :param element: FQN of the requirement, constraint, object or case asked about
    :param name: The asset name; defaults to ``<model file name>:<element>``
    :param opensysml_conn_id: The ``opensysml`` connection the watcher loads the model through
    :param strict: Refuse a model the service reports errors for
    :param patterns: Glob patterns selecting files under a directory
    :param poll_interval: Seconds between the watcher's digests
    :param settle_interval: Seconds a change must hold still before the model is re-asked
    :param watcher_name: The watcher's name; defaults to ``<name>_satisfied``
    :param asset_kwargs: Passed through to :class:`~airflow.sdk.Asset` (``group``, ``extra``, ...)
    """
    resolved = Path(path).expanduser().absolute()
    sysml_question = SysMLQuestion(
        kind,
        element,
        subject=subject,
        engine=engine,
        question=question,
        arguments=None if arguments is None else list(arguments),
        named_arguments=named_arguments,
        schedule=schedule,
    )
    identity = question_digest(sysml_question)
    asset_name = name or asset_name_for(resolved, element)
    if sysml_question != SysMLQuestion("requirement", element):
        asset_name = f"{asset_name}_{identity}"
    trigger = SysMLRequirementSatisfiedTrigger(
        model_path=str(resolved),
        question=sysml_question.as_dict(),
        opensysml_conn_id=opensysml_conn_id,
        strict=strict,
        patterns=patterns,
        poll_interval=poll_interval,
        settle_interval=settle_interval,
    )
    watcher = AssetWatcher(name=watcher_name or f"{asset_name}_satisfied", trigger=trigger)
    uri = f"{resolved.as_uri()}?satisfies={quote(element or '*', safe='')}&question={identity}"
    return Asset(name=asset_name, uri=uri, watchers=[watcher], **asset_kwargs)


def question_digest(question: SysMLQuestion) -> str:
    """A short digest of all that decides a question's answer, so distinct questions are distinct assets."""
    canonical = json.dumps(question.as_dict(), sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()[:12]
