from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from airflow.sdk import DAG

from airflow_provider_opensysml.dag import (
    ActionStep,
    KubernetesPodFactory,
    SysMLActionDag,
    conf_arguments,
    empty_task_factory,
    literal,
    load_graphs,
)

GRAPHS = Path(__file__).parent.parent / "example_dags" / "terrain_ncam" / "terrain_ncam.graphs.json"

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


def test_an_unset_behavior_attribute_keeps_the_subjects_value():
    graphs = {
        "version": 1,
        "subject": "P::Outer",
        "actions": [
            {
                "name": "P::Outer",
                "kind": "actionDef",
                "attributes": [{"name": "image", "types": ["String"], "value": {"text": '"worker:1"'}}],
                "nodes": [{"id": 0, "kind": "action usage", "name": "run", "performs": ["P::Inner"]}],
            },
            {
                "name": "P::Inner",
                "kind": "actionDef",
                "attributes": [
                    {"name": "image", "types": ["String"]},
                    {"name": "wrapper", "types": ["String"]},
                    {"name": "cpu", "types": ["ScalarValues::Integer"], "value": {"text": "2"}},
                ],
                "nodes": [],
            },
        ],
    }
    (step,) = SysMLActionDag(graphs, task_factory=empty_task_factory).steps
    assert step.attributes == {"image": "worker:1", "wrapper": None, "cpu": 2}


@pytest.mark.parametrize(
    ("graphs", "message"),
    [
        ({"version": 2, "subject": "S", "actions": [{"name": "S", "nodes": []}]}, "version 2"),
        ({"version": 1, "subject": "S", "actions": []}, "no action graph"),
        (
            {"version": 1, "subject": "S", "actions": [{"name": "Other", "nodes": []}]},
            "no action graph for its subject 'S'",
        ),
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
