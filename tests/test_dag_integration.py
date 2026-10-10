"""Live export of the terrain model through the hook, against a running sysml-grpc."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from airflow_provider_opensysml.dag import SysMLActionDag, empty_task_factory
from airflow_provider_opensysml.hooks import OpenSysMLHook
from test_dag import GRAPHS, TIG_EDGES, TIG_TASKS, edges_of

pytestmark = pytest.mark.integration

MODEL = Path(__file__).parent.parent / "example_dags" / "terrain_ncam" / "terrain_ncam.sysml"
SUBJECT = "TerrainNCAM::Pipeline::Terrain"


def test_live_export_builds_the_tig_dag():
    terrain = SysMLActionDag.from_model(str(MODEL), SUBJECT, task_factory=empty_task_factory)
    dag = terrain.dag("terrain_live", start_date=datetime(2026, 1, 1), schedule=None)
    assert set(dag.task_ids) == TIG_TASKS
    assert edges_of(dag) == TIG_EDGES


def test_live_export_agrees_with_the_checked_in_fixture():
    hook = OpenSysMLHook()
    connection = hook.get_conn()
    try:
        model = hook.load(connection, str(MODEL))
        graphs = model.export_graphs(SUBJECT)
    finally:
        connection.close()
    assert graphs.version == 1
    assert graphs.subject == SUBJECT
    live = SysMLActionDag(graphs.content, task_factory=empty_task_factory)
    checked_in = SysMLActionDag.from_file(GRAPHS, task_factory=empty_task_factory)
    assert live.task_ids == checked_in.task_ids
    assert live.dependencies == checked_in.dependencies
    assert [s.attributes for s in live.steps] == [s.attributes for s in checked_in.steps]


def test_model_requirements_hold():
    hook = OpenSysMLHook()
    connection = hook.get_conn()
    try:
        model = hook.load(connection, str(MODEL))
        verdicts = model.verify_satisfaction()
    finally:
        connection.close()
    assert verdicts
    assert all(v.holds for v in verdicts), [v for v in verdicts if not v.holds]
