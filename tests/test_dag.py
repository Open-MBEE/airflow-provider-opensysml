from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from airflow_provider_opensysml.dag import (
    ActionStep,
    KubernetesPodFactory,
    RequirementGate,
    RequirementGateFactory,
    SysMLActionDag,
    conf_arguments,
    empty_task_factory,
    gate_task_id,
    literal,
    load_gates,
    load_graphs,
)

GRAPHS = Path(__file__).parent.parent / "example_dags" / "models" / "terrain_ncam.graphs.json"

# The tasks and edges of NASA-AMMOS/tig, examples/airflow-k8s-pipeline/dags/ids_terrain_ncam.py:
#     [rad_left, rad_right] >> correlate_left
#     [rad_left, rad_right] >> correlate_right
#     correlate_left >> xyz_left >> mesh_left
#     correlate_right >> xyz_right >> mesh_right
TIG_TASKS = {
    "rad_left",
    "rad_right",
    "correlate_left",
    "correlate_right",
    "xyz_left",
    "xyz_right",
    "mesh_left",
    "mesh_right",
}
TIG_EDGES = {
    ("rad_left", "correlate_left"),
    ("rad_right", "correlate_left"),
    ("rad_left", "correlate_right"),
    ("rad_right", "correlate_right"),
    ("correlate_left", "xyz_left"),
    ("correlate_right", "xyz_right"),
    ("xyz_left", "mesh_left"),
    ("xyz_right", "mesh_right"),
}


def edges_of(dag: DAG) -> set[tuple[str, str]]:
    return {(task.task_id, downstream) for task in dag.tasks for downstream in task.downstream_task_ids}


@pytest.fixture
def terrain() -> SysMLActionDag:
    return SysMLActionDag.from_file(GRAPHS, task_factory=empty_task_factory)


def test_generated_dag_matches_the_tig_hand_written_dag(terrain: SysMLActionDag):
    dag = terrain.dag("terrain", start_date=datetime(2026, 1, 1), schedule=None)
    assert set(dag.task_ids) == TIG_TASKS
    assert edges_of(dag) == TIG_EDGES


def test_steps_carry_the_behaviors_attributes_pins_and_flows(terrain: SysMLActionDag):
    assert terrain.subject == "TerrainNCAM::Pipeline::Terrain"
    correlate = terrain.step("correlate_left")
    assert correlate.behavior == "TerrainNCAM::Steps::Correlate"
    assert correlate.attributes == {
        "image": "tig-worker:latest",
        "wrapper": "correlate_wrapper.sh",
        "cpu": 2,
        "memoryGi": 4,
        "ompThreads": 2,
    }
    assert correlate.inputs == {
        "eye": "TerrainNCAM::Products::RAS",
        "other": "TerrainNCAM::Products::RAS",
    }
    assert correlate.outputs == {"disparity": "TerrainNCAM::Products::DSP"}
    assert correlate.pin_source("eye").producer == "rad_left"
    assert correlate.pin_source("other").producer == "rad_right"
    assert "ompThreads" not in terrain.step("rad_left").attributes
    assert terrain.step("rad_left").attributes["image"] == "tig-worker:latest"


def test_flows_only_add_edges_a_succession_does_not_imply():
    graphs = {
        "version": 1,
        "subject": "P::Flow",
        "actions": [
            {
                "name": "P::Flow",
                "kind": "actionDef",
                "nodes": [
                    {"id": 0, "kind": "start"},
                    {"id": 1, "kind": "action usage", "name": "a"},
                    {"id": 2, "kind": "action usage", "name": "b"},
                    {"id": 3, "kind": "action usage", "name": "c"},
                    {"id": 4, "kind": "end"},
                ],
                "edges": [{"source": 0, "target": 1}, {"source": 1, "target": 2}, {"source": 2, "target": 4}],
                "flows": [
                    {"kind": "streaming", "source": 1, "sourcePin": "x", "target": 3, "targetPin": "x"},
                    {"kind": "streaming", "source": 1, "sourcePin": "x", "target": 2, "targetPin": "x"},
                ],
            }
        ],
    }
    generated = SysMLActionDag(graphs, task_factory=empty_task_factory)
    assert generated.dependencies == [("a", "b"), ("a", "c")]


def test_fork_and_join_fan_out_and_in():
    graphs = {
        "version": 1,
        "subject": "P::Split",
        "actions": [
            {
                "name": "P::Split",
                "kind": "actionDef",
                "nodes": [
                    {"id": 0, "kind": "start"},
                    {"id": 1, "kind": "action usage", "name": "prepare"},
                    {"id": 2, "kind": "fork", "name": "split"},
                    {"id": 3, "kind": "action usage", "name": "left"},
                    {"id": 4, "kind": "action usage", "name": "right"},
                    {"id": 5, "kind": "join", "name": "sync"},
                    {"id": 6, "kind": "action usage", "name": "publish"},
                    {"id": 7, "kind": "end"},
                ],
                "edges": [
                    {"source": 0, "target": 1},
                    {"source": 1, "target": 2},
                    {"source": 2, "target": 3},
                    {"source": 2, "target": 4},
                    {"source": 3, "target": 5},
                    {"source": 4, "target": 5},
                    {"source": 5, "target": 6},
                    {"source": 6, "target": 7},
                ],
            }
        ],
    }
    generated = SysMLActionDag(graphs, task_factory=empty_task_factory)
    assert generated.task_ids == ["prepare", "left", "right", "publish"]
    assert set(generated.dependencies) == {
        ("prepare", "left"),
        ("prepare", "right"),
        ("left", "publish"),
        ("right", "publish"),
    }


def test_performed_behavior_is_one_task_and_its_graph_is_not_expanded():
    graphs = {
        "version": 1,
        "subject": "P::Outer",
        "actions": [
            {
                "name": "P::Outer",
                "kind": "actionDef",
                "attributes": [{"name": "image", "types": ["String"], "value": {"text": '"img:1"'}}],
                "nodes": [
                    {"id": 0, "kind": "start"},
                    {"id": 1, "kind": "action usage", "name": "run", "performs": ["P::Inner"]},
                    {"id": 2, "kind": "end"},
                ],
                "edges": [{"source": 0, "target": 1}, {"source": 1, "target": 2}],
            },
            {
                "name": "P::Inner",
                "kind": "actionDef",
                "attributes": [
                    {"name": "cpu", "types": ["ScalarValues::Integer"], "value": {"text": "3"}},
                    {"name": "x", "types": ["ScalarValues::Real"]},
                ],
                "parameters": [{"name": "x", "direction": "in", "types": ["ScalarValues::Real"]}],
                "nodes": [
                    {"id": 0, "kind": "start"},
                    {"id": 1, "kind": "action usage", "name": "first"},
                    {"id": 2, "kind": "action usage", "name": "second"},
                    {"id": 3, "kind": "end"},
                ],
                "edges": [{"source": 0, "target": 1}, {"source": 1, "target": 2}, {"source": 2, "target": 3}],
            },
        ],
    }
    generated = SysMLActionDag(graphs, task_factory=empty_task_factory)
    (step,) = generated.steps
    assert step.task_id == "run"
    assert step.attributes == {"image": "img:1", "cpu": 3}
    assert step.inputs == {"x": "ScalarValues::Real"}
    assert generated.dependencies == []


@pytest.mark.parametrize(
    ("graphs", "message"),
    [
        ({"version": 2, "subject": "S", "actions": [{"name": "S", "nodes": []}]}, "version 2"),
        ({"version": 1, "subject": "S", "actions": []}, "no action graph"),
        (
            {
                "version": 1,
                "subject": "S",
                "actions": [{"name": "S", "nodes": [{"id": 0, "kind": "assignment", "name": "x"}]}],
            },
            "cannot become a task",
        ),
        (
            {
                "version": 1,
                "subject": "S",
                "actions": [
                    {
                        "name": "S",
                        "nodes": [
                            {"id": 0, "kind": "action usage", "name": "x"},
                            {"id": 1, "kind": "action usage", "name": "x"},
                        ],
                    }
                ],
            },
            "both named",
        ),
    ],
)
def test_unsupported_graphs_are_refused(graphs: dict, message: str):
    with pytest.raises(ValueError, match=message):
        SysMLActionDag(graphs, task_factory=empty_task_factory)


def test_load_graphs_accepts_text_path_and_mapping(tmp_path: Path):
    text = GRAPHS.read_text()
    copy = tmp_path / "g.json"
    copy.write_text(text)
    loaded = (load_graphs(text), load_graphs(str(copy)), load_graphs(json.loads(text)))
    subjects = {graphs["subject"] for graphs in loaded}
    assert subjects == {"TerrainNCAM::Pipeline::Terrain"}


@pytest.mark.parametrize(
    ("text", "value"),
    [('"a b"', "a b"), ("2", 2), ("2.5", 2.5), ("true", True), ("false", False), ("Eye::left", "Eye::left")],
)
def test_literal(text: str | None, value: object):
    assert literal(text) == value


def test_kubernetes_pod_factory_maps_attributes_to_the_pod():
    step = ActionStep(
        task_id="correlate_left",
        kind="action usage",
        performs=("P::Correlate",),
        attributes={"image": "tig-worker:latest", "wrapper": "correlate_wrapper.sh", "cpu": 2, "memoryGi": 4},
        inputs={"eye": "P::RAS", "other": "P::RAS"},
        outputs={"disparity": "P::DSP"},
    )
    factory = KubernetesPodFactory(pod_kwargs={"namespace": "tig-airflow"})
    with DAG("pods", start_date=datetime(2026, 1, 1), schedule=None) as dag:
        pod = factory(step, dag)
    assert pod.task_id == "correlate_left"
    assert pod.name == "correlate-left"
    assert pod.namespace == "tig-airflow"
    assert pod.image == "tig-worker:latest"
    assert pod.cmds == ["/bin/bash"]
    assert pod.arguments == [
        "/opt/wrappers/correlate_wrapper.sh",
        "{{ dag_run.conf['correlate_left.eye'] }}",
        "{{ dag_run.conf['correlate_left.other'] }}",
        "{{ dag_run.conf['correlate_left.disparity'] }}",
    ]
    assert pod.container_resources.requests == {"cpu": "2", "memory": "4Gi"}
    assert pod.container_resources.limits == {"cpu": "2", "memory": "4Gi"}
    assert conf_arguments(step) == pod.arguments[1:]


def test_kubernetes_pod_factory_needs_image_and_wrapper():
    step = ActionStep("x", "action usage", (), {"cpu": 1}, {}, {})
    with DAG("pods", start_date=datetime(2026, 1, 1), schedule=None) as dag:
        with pytest.raises(ValueError, match="image"):
            KubernetesPodFactory()(step, dag)


# --- Requirement gates ---------------------------------------------------------

GATES = GRAPHS.with_name("terrain_ncam.gates.json")


def test_gates_hold_back_the_step_they_name_or_the_first_steps():
    gates = load_gates(GATES)
    assert {g.label for g in gates} >= {"TN-1", "TN-3", "TN-8", "TN-9", "TN-10"}
    tn3 = next(g for g in gates if g.short_name == "TN-3")
    assert tn3.requirement == "TerrainNCAM::Requirements::correlateLeftCpu"
    assert tn3.satisfying_feature == "TerrainNCAM::Pipeline::Terrain::correlate_left"
    assert RequirementGate.from_dict(tn3.as_dict()) == tn3

    built: list[RequirementGate] = []

    def gate_factory(gate: RequirementGate, dag: DAG):
        built.append(gate)
        return EmptyOperator(task_id=gate_task_id(gate), dag=dag)

    terrain = SysMLActionDag.from_file(
        GRAPHS, task_factory=empty_task_factory, gates=gates, gate_factory=gate_factory
    )
    assert terrain.first_steps == ["rad_left", "rad_right"]
    assert terrain.gated_steps(tn3) == ["correlate_left"]
    tn9 = next(g for g in gates if g.short_name == "TN-9")
    assert terrain.gated_steps(tn9) == ["rad_left", "rad_right"]

    dag = terrain.dag("gated", start_date=datetime(2026, 1, 1), schedule=None)
    assert built == gates
    edges = edges_of(dag)
    assert TIG_EDGES <= edges
    assert ("require_TN-3", "correlate_left") in edges
    assert ("require_TN-9", "rad_left") in edges and ("require_TN-9", "rad_right") in edges
    assert ("require_TN-10", "rad_left") in edges
    assert {e for e in edges if e[0] == "require_TN-7"} == {("require_TN-7", "mesh_left")}


def test_gates_need_a_factory():
    gate = RequirementGate("R::x", "TerrainNCAM::Pipeline::Terrain::rad_left", "X-1")
    terrain = SysMLActionDag.from_file(GRAPHS, task_factory=empty_task_factory, gates=[gate])
    with pytest.raises(ValueError, match="gate_factory"):
        terrain.dag("ungated", start_date=datetime(2026, 1, 1), schedule=None)


def test_requirement_gate_factory_builds_a_sensor():
    from airflow_provider_opensysml.sensors import SysMLRequirementSensor

    gate = RequirementGate("R::x", "TerrainNCAM::Pipeline::Terrain::rad_left", "X 1")
    factory = RequirementGateFactory("/m/t.sysml", deferrable=True, poke_interval=5, opensysml_conn_id="svc")
    with DAG("g", start_date=datetime(2026, 1, 1), schedule=None) as dag:
        sensor = factory(gate, dag)
    assert isinstance(sensor, SysMLRequirementSensor)
    assert sensor.task_id == "require_X_1"
    assert sensor.model_path == "/m/t.sysml" and sensor.element == "R::x" and sensor.kind == "requirement"
    assert sensor.deferrable is True and sensor.poke_interval == 5 and sensor.opensysml_conn_id == "svc"
