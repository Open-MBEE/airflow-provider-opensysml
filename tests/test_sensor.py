from __future__ import annotations

import pytest
from airflow.sdk.bases.sensor import BaseSensorOperator, PokeReturnValue
from airflow.sdk.exceptions import AirflowException, TaskDeferred

from airflow_provider_opensysml.questions import Answer, SysMLQuestion
from airflow_provider_opensysml.sensors import SysMLRequirementSensor
from airflow_provider_opensysml.triggers.requirement import SysMLRequirementHoldsTrigger

HOLDS = Answer(True, {"holds": True})
FALSE = Answer(False, {"holds": False}, ["requirement R does not hold (x)"])
UNDECIDED = Answer(False, {"holds": False}, error="requirement R could not be decided: no engine")


@pytest.fixture
def answers(monkeypatch: pytest.MonkeyPatch):
    """Script what the model answers; records each question asked."""

    class Script(list):
        asked: list[tuple[str, SysMLQuestion, str, bool]] = []

    script = Script()

    def ask_model(model_path, question, conn_id, strict):
        script.asked.append((model_path, question, conn_id, strict))
        return script.pop(0)

    monkeypatch.setattr("airflow_provider_opensysml.sensors.sysml.ask_model", ask_model)
    return script


def sensor(**kwargs) -> SysMLRequirementSensor:
    defaults = dict(task_id="gate", model_path="/m/t.sysml", element="R", poke_interval=0.01, timeout=5)
    return SysMLRequirementSensor(**{**defaults, **kwargs})


def test_is_a_sensor_asking_a_question():
    s = sensor(kind="case", element="V::check", named_arguments={"limit": "{{ dag_run.conf['limit'] }}"})
    assert isinstance(s, BaseSensorOperator)
    assert "named_arguments" in s.template_fields and "model_path" in s.template_fields
    assert s.sysml_question() == SysMLQuestion(
        "case", "V::check", named_arguments={"limit": "{{ dag_run.conf['limit'] }}"}
    )
    with pytest.raises(ValueError):
        sensor(kind="requirement", element=None)


def test_poke_until_it_holds(answers):
    answers.extend([FALSE, HOLDS])
    s = sensor(deferrable=False)
    assert s.poke({}) is False
    report = s.execute({})
    assert report == {"holds": True}
    assert [a[1] for a in answers.asked] == [SysMLQuestion("requirement", "R")] * 2
    assert answers.asked[0][0] == "/m/t.sysml" and answers.asked[0][3] is True


def test_undecided_fails_unless_told_to_wait(answers):
    answers.append(UNDECIDED)
    with pytest.raises(AirflowException, match="could not be decided"):
        sensor(deferrable=False).poke({})
    answers.extend([UNDECIDED, HOLDS])
    s = sensor(deferrable=False, fail_on_undecided=False)
    assert s.poke({}) is False
    assert isinstance(s.poke({}), PokeReturnValue)


def test_defers_to_the_holds_trigger_when_not_yet_holding(answers):
    answers.append(FALSE)
    s = sensor(deferrable=True, settle_interval=0.5, opensysml_conn_id="svc")
    with pytest.raises(TaskDeferred) as deferred:
        s.execute({})
    trigger = deferred.value.trigger
    assert isinstance(trigger, SysMLRequirementHoldsTrigger)
    assert deferred.value.method_name == "execute_complete"
    classpath, kwargs = trigger.serialize()
    assert classpath.endswith("SysMLRequirementHoldsTrigger")
    assert kwargs["model_path"] == "/m/t.sysml" and kwargs["opensysml_conn_id"] == "svc"
    assert kwargs["question"]["element"] == "R" and kwargs["poll_interval"] == 0.01
    assert kwargs["fail_on_undecided"] is True
    answers.append(FALSE)
    with pytest.raises(TaskDeferred) as lenient:
        sensor(deferrable=True, fail_on_undecided=False).execute({})
    assert lenient.value.trigger.serialize()[1]["fail_on_undecided"] is False
    assert kwargs["settle_interval"] == 0.5


def test_deferrable_returns_at_once_when_it_already_holds(answers):
    answers.append(HOLDS)
    assert sensor(deferrable=True).execute({}) == {"holds": True}


def test_execute_complete_reads_the_event():
    s = sensor()
    assert s.execute_complete({}, {"holds": True, "report": {"holds": True}, "digest": "d"}) == {
        "holds": True
    }
    with pytest.raises(AirflowException, match="no engine"):
        s.execute_complete({}, {"holds": False, "error": "no engine"})
    with pytest.raises(AirflowException):
        s.execute_complete({}, None)
