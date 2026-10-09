"""Operator tests against a real ``sysml-grpc`` service.

The ``opensysml`` client starts a private service, resolving its binary as the
package documents: ``$OPENSYSML_BINARY``, then its cache, then a download of the
release it pins. Set ``OPENSYSML_BINARY`` to a local build to test against that.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.sdk.exceptions import AirflowException

from airflow_provider_opensysml.hooks import OpenSysMLHook
from airflow_provider_opensysml.operators import SysMLAnalysisOperator, SysMLVerifyOperator

pytestmark = pytest.mark.integration


def test_hook_without_connection_starts_private_service(lander: Path):
    hook = OpenSysMLHook()
    connection = hook.get_conn()
    try:
        model = hook.load(connection, str(lander))
        assert model.documents
    finally:
        connection.close()


def test_hook_connection_names_external_service(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(
        "AIRFLOW_CONN_OPENSYSML_TEST", '{"conn_type": "opensysml", "host": "grpc.example", "port": 50099}'
    )
    hook = OpenSysMLHook("opensysml_test")
    assert hook._connection_settings() == ("grpc.example", 50099, {})


def test_analysis_operator_reports_outputs_and_verdicts(lander: Path):
    report = SysMLAnalysisOperator(
        task_id="budget", model_path=str(lander), case="Descent::scoutBudget"
    ).execute({})
    assert report["outputs"]["fuelUsed"] == 120.0
    assert report["outputs"]["wetMass"] == 730.0
    (verdict,) = report["verdicts"]
    assert verdict["kind"] == "objective" and verdict["holds"] is True
    assert report["standing"]["engine"]


def test_verification_case_reports_pass(lander: Path):
    report = SysMLAnalysisOperator(
        task_id="check", model_path=str(lander), case="Descent::checkScout"
    ).execute({})
    assert [v["kind"] for v in report["verifications"]] == ["pass"]


def test_verify_requirement(lander: Path):
    op = SysMLVerifyOperator(
        task_id="soft", model_path=str(lander), kind="requirement", element="Descent::scoutLandsSoftly"
    )
    report = op.execute({})
    assert report["holds"] is True and report["kind"] == "requirement"
    assert report["verifications"][0]["kind"] == "pass"


def test_false_answer_fails_the_task(lander: Path, lander_source: str):
    # A scout burning ten times faster uses more fuel than it carries, so the reserve objective fails.
    broken = lander_source.replace("attribute :>> burnRate = 3.0;", "attribute :>> burnRate = 30.0;")
    assert broken != lander_source
    lander.write_text(broken)
    op = SysMLAnalysisOperator(task_id="budget", model_path=str(lander), case="Descent::scoutBudget")
    with pytest.raises(AirflowException, match="does not hold"):
        op.execute({})

    lenient = SysMLAnalysisOperator(
        task_id="budget", model_path=str(lander), case="Descent::scoutBudget", fail_on_verdict=False
    )
    report = lenient.execute({})
    assert report["verdicts"][0]["holds"] is False


def test_unknown_case_is_undecided_and_fails_with_the_clients_message(lander: Path):
    op = SysMLAnalysisOperator(task_id="nope", model_path=str(lander), case="Descent::noSuchCase")
    with pytest.raises(AirflowException, match="could not be decided.*noSuchCase"):
        op.execute({})


def test_verify_operator_argument_checks():
    with pytest.raises(ValueError):
        SysMLVerifyOperator(task_id="x", model_path="m.sysml", kind="bogus", element="E")
    with pytest.raises(ValueError):
        SysMLVerifyOperator(task_id="x", model_path="m.sysml", kind="constraint")
    with pytest.raises(ValueError):
        SysMLVerifyOperator(task_id="x", model_path="m.sysml", kind="object", element="E", subject="S")
