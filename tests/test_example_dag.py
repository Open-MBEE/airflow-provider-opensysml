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
    assert asset.uri.endswith("/example_dags/models/lander.sysml")
    assert asset.watchers[0].trigger.serialize()[0].endswith("SysMLModelChangedTrigger")


def test_terrain_example_dag_is_generated_from_the_model():
    bag = DagBag(dag_folder=str(EXAMPLES), safe_mode=False)
    assert bag.import_errors == {}
    dag = bag.dags["opensysml_terrain_ncam"]
    assert type(dag.timetable).__name__ == "NullTimetable"
    gates = {f"require_TN-{n}" for n in range(1, 11)} | {"require_TN-12"}
    assert set(dag.task_ids) == TIG_TASKS | gates
    edges = edges_of(dag)
    assert {e for e in edges if not e[0].startswith("require_")} == TIG_EDGES
    assert ("require_TN-3", "correlate_left") in edges and ("require_TN-7", "mesh_left") in edges
    assert ("require_TN-9", "rad_left") in edges and ("require_TN-9", "rad_right") in edges
    assert {e[1] for e in edges if e[0] == "require_TN-12"} == {"correlate_left", "correlate_right"}
    stereo = dag.get_task("require_TN-12")
    assert stereo.kind == "case" and stereo.element == "TerrainNCAM::Verification::stereoPairCheck"
    assert stereo.named_arguments["leftEye"] == "{{ eye(dag_run.conf['left_key']) }}"
    macros = dag.user_defined_macros
    key = "input/NLM_1835_0829848458_777FDR_N0874924NCAM00230_0A02LLJ01.VIC"
    assert macros["eye"](key) == "L" and macros["eye"](key.replace("NLM", "NRM")) == "R"
    assert macros["acquisition"](key) == macros["acquisition"](key.replace("NLM", "NRM"))
    assert macros["acquisition"](key) == "1835_0829848458_777_N0874924NCAM00230_0A02LLJ01"
    assert dag.get_task("require_TN-3").element == "TerrainNCAM::Requirements::correlateLeftCpu"
    correlate_right = dag.get_task("correlate_right")
    assert correlate_right.image == "tig-worker:latest"
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
