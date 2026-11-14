"""The terrain model's document queries and report, against a running sysml-grpc."""

from __future__ import annotations

from pathlib import Path

import pytest
from opensysml import InvalidRequestError

from airflow_provider_opensysml.hooks import OpenSysMLHook
from test_dag import TIG_TASKS

pytestmark = pytest.mark.integration

MODEL = Path(__file__).parent.parent / "example_dags" / "terrain_ncam" / "terrain_ncam.sysml"
REPORT = "TerrainNCAM::Report::TerrainReport"
STEP_TABLE = "TerrainNCAM::Report::StepTable"
EYE_DIFFERENCE = "TerrainNCAM::Report::EyeCpuDifference"


@pytest.fixture
def load(tmp_path: Path):
    hook = OpenSysMLHook()
    connection = hook.get_conn()

    def _load(source: str | None = None):
        path = MODEL
        if source is not None:
            path = tmp_path / "terrain_ncam.sysml"
            path.write_text(source)
        return hook.load(connection, str(path))

    yield _load
    connection.close()


def test_step_table_lists_exactly_the_tasks_of_the_dag(load):
    result = load().run_document_query(STEP_TABLE)
    names = {row.cells[result.columns.index("name")][0] for row in result.rows}
    assert names == TIG_TASKS


def test_a_step_without_a_sizing_fails_the_step_table(load):
    source = MODEL.read_text().replace(
        "action mesh_right : MeshStep;", "action mesh_right : MeshStep;\n            action publish;"
    )
    with pytest.raises(InvalidRequestError, match="wrapper of .*publish"):
        load(source).run_document_query(STEP_TABLE)


def test_eyes_are_sized_alike_and_a_difference_is_reported(load):
    assert load().run_document_query(EYE_DIFFERENCE).rows == ()
    source = MODEL.read_text().replace(
        "action mesh_right : MeshStep;", "action mesh_right : MeshStep { attribute :>> cpu = 3; }"
    )
    result = load(source).run_document_query(EYE_DIFFERENCE)
    assert [tuple(cell[0] for cell in row.cells) for row in result.rows] == [
        ("TerrainNCAM::Steps::MeshStep", 2, 3)
    ]


def test_report_carries_the_step_table_and_the_eye_comparison(load):
    report = load().render_document(REPORT)
    assert "| mesh_right |" in report.replace("\\", "")
    assert "Step types whose cpu limit differs between the eyes" in report
