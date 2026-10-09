from __future__ import annotations

from pathlib import Path

from airflow.dag_processing.dagbag import DagBag

from test_dag import TIG_EDGES, TIG_TASKS, edges_of

EXAMPLES = Path(__file__).parent.parent / "example_dags"


def test_example_dag_imports_and_schedules_on_the_model():
    bag = DagBag(dag_folder=str(EXAMPLES), safe_mode=False)
    assert bag.import_errors == {}
    dag = bag.dags["opensysml_lander_verification"]
    assert set(dag.task_ids) == {"fuel_budget", "soft_landing", "touchdown_check", "summarize"}
    (asset,) = dag.timetable.asset_condition.objects
    assert asset.name == "lander_model"
    assert asset.uri.endswith("/example_dags/lander_verification/lander.sysml")
    assert asset.watchers[0].trigger.serialize()[0].endswith("SysMLModelChangedTrigger")


def test_terrain_example_dag_is_generated_from_the_model():
    bag = DagBag(dag_folder=str(EXAMPLES), safe_mode=False)
    assert bag.import_errors == {}
    dag = bag.dags["opensysml_terrain_ncam"]
    assert type(dag.timetable).__name__ == "NullTimetable"
    assert set(dag.task_ids) == TIG_TASKS
    assert edges_of(dag) == TIG_EDGES
    correlate_right = dag.get_task("correlate_right")
    assert correlate_right.image == "tig-worker:latest"
    env = {var.name: var.value for var in correlate_right.env_vars}
    assert env["AWS_ACCESS_KEY_ID"] == "{{ var.value.tig_s3_access_key }}"
    assert env["AWS_SECRET_ACCESS_KEY"] == "{{ var.value.tig_s3_secret_key }}"
    assert correlate_right.container_resources.limits == {"cpu": "2", "memory": "4Gi"}
    assert correlate_right.arguments[:3] == [
        "/opt/wrappers/correlate_wrapper.sh",
        "http://minio.tig-airflow.svc.cluster.local:9000",
        "{{ dag_run.conf['bucket'] }}",
    ]
    assert correlate_right.arguments[3:] == [
        "processque/{{ run_id }}/{{ fdr_to_ras_base(dag_run.conf['right_key']) }}.VIC",
        "processque/{{ run_id }}/{{ fdr_to_ras_base(dag_run.conf['left_key']) }}.VIC",
        "processque/{{ run_id }}/"
        "{{ product_name(fdr_to_ras_base(dag_run.conf['right_key']), 'DSP', 'img') }}",
        "{{ run_id }}",
        "right",
    ]
    mesh_left = dag.get_task("mesh_left")
    assert mesh_left.arguments[0] == "/opt/wrappers/mesh_wrapper.sh"
    assert mesh_left.arguments[3:] == [
        "processque/{{ run_id }}/{{ product_name(fdr_to_ras_base(dag_run.conf['left_key']), 'XYM', 'xym') }}",
        "processque/{{ run_id }}/{{ fdr_to_ras_base(dag_run.conf['left_key']) }}.VIC",
        "{{ ods_prefix(fdr_to_ras_base(dag_run.conf['left_key'])) }}",
        "{{ fdr_to_ras_base(dag_run.conf['left_key']) }}",
        "{{ run_id }}",
        "left",
    ]
