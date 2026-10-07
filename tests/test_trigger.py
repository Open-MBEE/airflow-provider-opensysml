from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from airflow.triggers.base import BaseEventTrigger, TriggerEvent

from airflow_provider_opensysml.digest import ABSENT, digest_model
from airflow_provider_opensysml.triggers.model import SysMLModelChangedTrigger

FAST = {"poll_interval": 0.05, "settle_interval": 0.05}


async def next_event(trigger: SysMLModelChangedTrigger, timeout: float = 5.0) -> TriggerEvent:
    return await asyncio.wait_for(trigger.run().__anext__(), timeout)


def test_is_an_event_trigger_and_serializes():
    trigger = SysMLModelChangedTrigger(path="/models/rover.sysml", poll_interval=10, settle_interval=1)
    assert isinstance(trigger, BaseEventTrigger)
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow_provider_opensysml.triggers.model.SysMLModelChangedTrigger"
    assert kwargs == {
        "path": "/models/rover.sysml",
        "patterns": ["*.sysml", "*.kerml"],
        "poll_interval": 10.0,
        "settle_interval": 1.0,
    }
    rehydrated = SysMLModelChangedTrigger(**kwargs)
    assert rehydrated.serialize() == trigger.serialize()


def test_rejects_bad_intervals():
    with pytest.raises(ValueError):
        SysMLModelChangedTrigger(path="x", poll_interval=0)
    with pytest.raises(ValueError):
        SysMLModelChangedTrigger(path="x", settle_interval=-1)


@pytest.mark.asyncio
async def test_fires_when_file_changes(tmp_path: Path):
    model = tmp_path / "m.sysml"
    model.write_text("package P;")
    before = digest_model(model).digest
    trigger = SysMLModelChangedTrigger(path=str(model), **FAST)

    async def edit():
        await asyncio.sleep(0.2)
        model.write_text("package P { part p; }")

    task = asyncio.ensure_future(edit())
    event = await next_event(trigger)
    await task
    assert event.payload["path"] == str(model)
    assert event.payload["previous_digest"] == before
    assert event.payload["digest"] == digest_model(model).digest
    assert event.payload["files"] == 1
    assert "observed_at" in event.payload


@pytest.mark.asyncio
async def test_silent_while_unchanged(tmp_path: Path):
    model = tmp_path / "m.sysml"
    model.write_text("package P;")
    trigger = SysMLModelChangedTrigger(path=str(model), **FAST)
    with pytest.raises(asyncio.TimeoutError):
        await next_event(trigger, timeout=0.5)


@pytest.mark.asyncio
async def test_rewrite_with_same_content_is_silent(tmp_path: Path):
    model = tmp_path / "m.sysml"
    model.write_text("package P;")
    trigger = SysMLModelChangedTrigger(path=str(model), **FAST)

    async def rewrite():
        await asyncio.sleep(0.15)
        model.write_text("package P;")

    task = asyncio.ensure_future(rewrite())
    with pytest.raises(asyncio.TimeoutError):
        await next_event(trigger, timeout=0.6)
    await task


@pytest.mark.asyncio
async def test_fires_once_for_a_burst_of_writes(tmp_path: Path):
    (tmp_path / "a.sysml").write_text("package A;")
    trigger = SysMLModelChangedTrigger(path=str(tmp_path), poll_interval=0.05, settle_interval=0.3)
    events: list[TriggerEvent] = []

    async def collect():
        async for event in trigger.run():
            events.append(event)

    collector = asyncio.ensure_future(collect())
    await asyncio.sleep(0.15)
    for i in range(5):
        (tmp_path / f"part{i}.sysml").write_text(f"package Part{i};")
        await asyncio.sleep(0.05)
    await asyncio.sleep(1.0)
    collector.cancel()
    assert len(events) == 1
    assert events[0].payload["files"] == 6


@pytest.mark.asyncio
async def test_appearance_and_removal_are_changes(tmp_path: Path):
    model = tmp_path / "later.sysml"
    trigger = SysMLModelChangedTrigger(path=str(model), **FAST)
    events: list[TriggerEvent] = []

    async def collect():
        async for event in trigger.run():
            events.append(event)
            if len(events) == 2:
                return

    collector = asyncio.ensure_future(collect())
    await asyncio.sleep(0.15)
    model.write_text("package P;")
    await asyncio.sleep(0.4)
    model.unlink()
    await asyncio.wait_for(collector, 5.0)
    assert events[0].payload["previous_digest"] == ABSENT
    assert events[0].payload["digest"].startswith("sha256:")
    assert events[1].payload["digest"] == ABSENT


def test_empty_patterns_select_nothing(tmp_path: Path):
    (tmp_path / "m.sysml").write_text("package M;")
    trigger = SysMLModelChangedTrigger(path=str(tmp_path), patterns=[])
    assert trigger.patterns == []
    assert trigger.serialize()[1]["patterns"] == []
    assert digest_model(tmp_path, trigger.patterns).digest == ABSENT
