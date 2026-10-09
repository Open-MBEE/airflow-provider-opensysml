"""Requirement gates against a running sysml-grpc: the sensor, the watcher and the satisfy statements."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.sdk.exceptions import AirflowException

from airflow_provider_opensysml.dag import gates_of_model, load_gates
from airflow_provider_opensysml.hooks import OpenSysMLHook
from airflow_provider_opensysml.questions import SysMLQuestion
from airflow_provider_opensysml.sensors import SysMLRequirementSensor
from airflow_provider_opensysml.triggers.requirement import SysMLRequirementSatisfiedTrigger, ask_model

pytestmark = pytest.mark.integration

MODELS = Path(__file__).parent.parent / "example_dags" / "models"
TERRAIN = MODELS / "terrain_ncam.sysml"
STEREO = "TerrainNCAM::Verification::stereoPairCheck"
PAIR = {"leftAcquisition": "1835_0829848458_777", "rightAcquisition": "1835_0829848458_777"}


def broken(lander_source: str) -> str:
    return lander_source.replace("attribute :>> burnRate = 3.0;", "attribute :>> burnRate = 30.0;")


def test_the_models_satisfy_statements_are_the_checked_in_gates():
    hook = OpenSysMLHook()
    connection = hook.get_conn()
    try:
        gates = gates_of_model(hook.load(connection, str(TERRAIN)))
    finally:
        connection.close()
    assert gates == load_gates(MODELS / "terrain_ncam.gates.json")
    assert [g.label for g in gates] == [f"TN-{n}" for n in range(1, 11)]
    assert gates[2].satisfying_feature == "TerrainNCAM::Pipeline::Terrain::correlate_left"


def test_a_case_is_asked_with_a_runs_values_bound():
    ask = lambda **named: ask_model(  # noqa: E731
        str(TERRAIN), SysMLQuestion("case", STEREO, named_arguments=named), "opensysml_default", True
    )
    assert ask(**PAIR, leftEye="L", rightEye="R").holds
    swapped = ask(**PAIR, leftEye="R", rightEye="L")
    assert not swapped.holds and swapped.decided and "stereoPairCheck" in swapped.failures[0]
    other = ask(leftAcquisition="a", rightAcquisition="b", leftEye="L", rightEye="R")
    assert not other.holds and other.decided
    unbound = ask(**PAIR, leftEye="L")
    assert not unbound.decided and "rightEye" in unbound.error


def test_sensor_waits_until_the_requirement_holds(lander: Path, lander_source: str):
    sensor = SysMLRequirementSensor(
        task_id="budget", model_path=str(lander), kind="case", element="Descent::scoutBudget", timeout=30
    )
    lander.write_text(broken(lander_source))
    assert sensor.poke({}) is False
    lander.write_text(lander_source)
    poked = sensor.poke({})
    assert isinstance(poked, PokeReturnValue) and poked.xcom_value["verdicts"][0]["holds"] is True
    missing = SysMLRequirementSensor(
        task_id="missing", model_path=str(lander), element="Descent::noSuchRequirement", timeout=30
    )
    with pytest.raises(AirflowException):
        missing.poke({})


@pytest.mark.asyncio
async def test_watcher_fires_when_the_model_comes_to_satisfy_the_requirement(
    lander: Path, lander_source: str
):
    lander.write_text(broken(lander_source))
    trigger = SysMLRequirementSatisfiedTrigger(
        model_path=str(lander),
        question={"kind": "case", "element": "Descent::scoutBudget"},
        poll_interval=0.2,
        settle_interval=0.2,
    )
    events = trigger.run()

    async def fix() -> None:
        await asyncio.sleep(1.0)
        lander.write_text(lander_source)

    task = asyncio.ensure_future(fix())
    event = await asyncio.wait_for(events.__anext__(), 60)
    await task
    assert event.payload["question"] == "case Descent::scoutBudget"
    assert event.payload["report"]["verdicts"][0]["holds"] is True
    json.dumps(event.payload)
