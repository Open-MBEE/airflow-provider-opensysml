"""Verify the lander model whenever it changes.

The model under ``models/`` is the ``analysis-demo`` lander from OpenSysML. The
DAG is scheduled on the asset :func:`sysml_model_asset` builds for it, so a
save, a checkout or a copy over the file starts a run that runs the fuel budget
analysis, verifies the soft-landing requirement and runs the verification case
that checks it. Set ``$OPENSYSML_EXAMPLE_MODEL`` to point the DAG at another
copy of the model, for instance one the triggerer and the workers share.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task

from airflow_provider_opensysml.assets import sysml_model_asset
from airflow_provider_opensysml.operators import SysMLAnalysisOperator, SysMLVerifyOperator

MODEL = os.environ.get("OPENSYSML_EXAMPLE_MODEL") or str(Path(__file__).parent / "models" / "lander.sysml")

lander = sysml_model_asset(MODEL, name="lander_model", poll_interval=15)


@dag(
    dag_id="opensysml_lander_verification",
    schedule=[lander],
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["opensysml", "example"],
)
def lander_verification():
    fuel_budget = SysMLAnalysisOperator(
        task_id="fuel_budget",
        model_path=MODEL,
        case="Descent::scoutBudget",
    )
    soft_landing = SysMLVerifyOperator(
        task_id="soft_landing",
        model_path=MODEL,
        kind="requirement",
        element="Descent::scoutLandsSoftly",
    )
    touchdown_check = SysMLAnalysisOperator(
        task_id="touchdown_check",
        model_path=MODEL,
        case="Descent::checkScout",
    )

    @task
    def summarize(budget: dict, check: dict, **context) -> str:
        events = context["triggering_asset_events"]
        changes = [
            (e.extra.get("payload") or e.extra).get("digest") for events_ in events.values() for e in events_
        ]
        lines = [f"model: {MODEL}", f"triggered by: {', '.join(c for c in changes if c) or 'manual run'}"]
        lines += [f"{name} = {value}" for name, value in budget["outputs"].items()]
        lines += [f"{v['case']}: {v['kind']}" for v in check["verifications"]]
        report = "\n".join(lines)
        print(report)
        return report

    [fuel_budget, soft_landing] >> touchdown_check
    summarize(fuel_budget.output, touchdown_check.output)


lander_verification()
