from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from airflow.triggers.base import BaseEventTrigger, BaseTrigger, TriggerEvent

from airflow_provider_opensysml.questions import Answer
from airflow_provider_opensysml.triggers.requirement import (
    SysMLRequirementHoldsTrigger,
    SysMLRequirementSatisfiedTrigger,
)

FAST = {"poll_interval": 0.05, "settle_interval": 0.05}
QUESTION = {"kind": "requirement", "element": "R"}
HOLDS = Answer(True, {"holds": True})
FALSE = Answer(False, {"holds": False}, ["requirement R does not hold (x)"])
UNDECIDED = Answer(False, {"holds": False}, error="requirement R could not be decided")


@pytest.fixture
def answers(monkeypatch: pytest.MonkeyPatch):
    """The model's answer is a function of its text, so edits flip the verdict."""
    by_text: dict[str, Answer] = {}
    asked: list[str] = []

    def ask_model(model_path, question, conn_id, strict):
        text = Path(model_path).read_text()
        asked.append(text)
        return by_text[text]

    monkeypatch.setattr("airflow_provider_opensysml.triggers.requirement.ask_model", ask_model)
    by_text["asked"] = asked  # type: ignore[assignment]
    return by_text


async def next_event(trigger, timeout: float = 5.0) -> TriggerEvent:
    return await asyncio.wait_for(trigger.run().__anext__(), timeout)


async def edit(model: Path, text: str, after: float = 0.2) -> None:
    await asyncio.sleep(after)
    model.write_text(text)


def test_serialize_and_validate():
    trigger = SysMLRequirementSatisfiedTrigger(model_path="/m/t.sysml", question=QUESTION, poll_interval=3)
    assert isinstance(trigger, BaseEventTrigger)
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow_provider_opensysml.triggers.requirement.SysMLRequirementSatisfiedTrigger"
    assert kwargs["question"]["kind"] == "requirement" and kwargs["question"]["element"] == "R"
    assert kwargs["patterns"] == ["*.sysml", "*.kerml"] and kwargs["poll_interval"] == 3.0
    assert SysMLRequirementSatisfiedTrigger(**kwargs).serialize() == trigger.serialize()
    holds = SysMLRequirementHoldsTrigger(**kwargs)
    assert isinstance(holds, BaseTrigger) and not isinstance(holds, BaseEventTrigger)
    with pytest.raises(ValueError):
        SysMLRequirementHoldsTrigger(model_path="x", question={"kind": "bogus", "element": "E"})
    with pytest.raises(ValueError):
        SysMLRequirementHoldsTrigger(model_path="x", question=QUESTION, poll_interval=0)


@pytest.mark.asyncio
async def test_holds_trigger_fires_at_once_when_it_holds(tmp_path: Path, answers):
    model = tmp_path / "m.sysml"
    model.write_text("good")
    answers["good"] = HOLDS
    event = await next_event(SysMLRequirementHoldsTrigger(model_path=str(model), question=QUESTION, **FAST))
    assert event.payload["holds"] is True and event.payload["report"] == {"holds": True}
    assert answers["asked"] == ["good"]


@pytest.mark.asyncio
async def test_holds_trigger_waits_for_the_model_to_change(tmp_path: Path, answers):
    model = tmp_path / "m.sysml"
    model.write_text("bad")
    answers.update(bad=FALSE, good=HOLDS)
    task = asyncio.ensure_future(edit(model, "good"))
    event = await next_event(SysMLRequirementHoldsTrigger(model_path=str(model), question=QUESTION, **FAST))
    await task
    assert event.payload["holds"] is True
    assert answers["asked"] == ["bad", "good"]


@pytest.mark.asyncio
async def test_holds_trigger_reports_an_undecided_answer(tmp_path: Path, answers):
    model = tmp_path / "m.sysml"
    model.write_text("odd")
    answers["odd"] = UNDECIDED
    event = await next_event(SysMLRequirementHoldsTrigger(model_path=str(model), question=QUESTION, **FAST))
    assert event.payload["holds"] is False and "could not be decided" in event.payload["error"]


@pytest.mark.asyncio
async def test_satisfied_trigger_fires_on_false_to_true_only(tmp_path: Path, answers):
    model = tmp_path / "m.sysml"
    model.write_text("bad")
    answers.update(bad=FALSE, good=HOLDS, better=HOLDS, odd=UNDECIDED)
    trigger = SysMLRequirementSatisfiedTrigger(model_path=str(model), question=QUESTION, **FAST)
    events = trigger.run()

    async def edits():
        await edit(model, "good", 0.2)
        await edit(model, "better", 0.3)
        await edit(model, "odd", 0.3)
        await edit(model, "good", 0.3)

    task = asyncio.ensure_future(edits())
    first = await asyncio.wait_for(events.__anext__(), 5)
    second = await asyncio.wait_for(events.__anext__(), 5)
    await task
    assert first.payload["question"] == "requirement R" and first.payload["report"] == {"holds": True}
    assert first.payload["path"] == str(model) and "observed_at" in first.payload
    # "good" -> "better" stayed true (no event); "odd" was undecided; "good" again is false -> true.
    assert answers["asked"] == ["bad", "good", "better", "odd", "good"]
    assert second.payload["digest"] != first.payload["digest"] or first.payload["digest"]


@pytest.mark.asyncio
async def test_satisfied_trigger_is_silent_for_a_model_that_already_holds(tmp_path: Path, answers):
    model = tmp_path / "m.sysml"
    model.write_text("good")
    answers.update(good=HOLDS, better=HOLDS)
    trigger = SysMLRequirementSatisfiedTrigger(model_path=str(model), question=QUESTION, **FAST)
    task = asyncio.ensure_future(edit(model, "better"))
    with pytest.raises(asyncio.TimeoutError):
        await next_event(trigger, timeout=0.8)
    await task
    assert answers["asked"] == ["good", "better"]
