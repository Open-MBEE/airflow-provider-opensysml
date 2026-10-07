from __future__ import annotations

from pathlib import Path

from airflow.dag_processing.dagbag import DagBag

EXAMPLES = Path(__file__).parent.parent / "example_dags"


def test_example_dag_imports_and_schedules_on_the_model():
    bag = DagBag(dag_folder=str(EXAMPLES), safe_mode=False)
    assert bag.import_errors == {}
    dag = bag.dags["opensysml_lander_verification"]
    assert set(dag.task_ids) == {"fuel_budget", "soft_landing", "touchdown_check", "summarize"}
    (asset,) = dag.timetable.asset_condition.objects
    assert asset.name == "lander_model"
    assert asset.uri.endswith("/example_dags/models/lander.sysml")
    assert asset.watchers[0].trigger.serialize()[0].endswith("SysMLModelChangedTrigger")
