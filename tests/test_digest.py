from __future__ import annotations

from pathlib import Path

from airflow_provider_opensysml.digest import ABSENT, digest_model, model_files


def test_single_file_digest_is_content_based(tmp_path: Path):
    f = tmp_path / "m.sysml"
    f.write_text("package P;")
    first = digest_model(f)
    assert first.present and first.files == 1 and first.digest.startswith("sha256:")

    f.write_text("package P;")
    assert digest_model(f) == first, "rewriting identical content is not a change"

    f.write_text("package Q;")
    assert digest_model(f).digest != first.digest


def test_absent_path(tmp_path: Path):
    missing = digest_model(tmp_path / "nowhere.sysml")
    assert missing.digest == ABSENT and missing.files == 0 and not missing.present


def test_directory_digest_covers_model_files_only(tmp_path: Path):
    (tmp_path / "a.sysml").write_text("package A;")
    (tmp_path / "lib").mkdir()
    (tmp_path / "lib" / "b.kerml").write_text("package B;")
    (tmp_path / "notes.txt").write_text("ignored")
    assert [f.name for f in model_files(tmp_path)] == ["a.sysml", "b.kerml"]

    before = digest_model(tmp_path)
    assert before.files == 2
    (tmp_path / "notes.txt").write_text("still ignored")
    assert digest_model(tmp_path) == before

    (tmp_path / "lib" / "b.kerml").write_text("package B2;")
    assert digest_model(tmp_path).digest != before.digest


def test_directory_digest_sees_renames_and_additions(tmp_path: Path):
    (tmp_path / "a.sysml").write_text("package A;")
    before = digest_model(tmp_path)
    (tmp_path / "a.sysml").rename(tmp_path / "b.sysml")
    renamed = digest_model(tmp_path)
    assert renamed.digest != before.digest and renamed.files == 1
    (tmp_path / "c.sysml").write_text("package C;")
    assert digest_model(tmp_path).files == 2


def test_custom_patterns(tmp_path: Path):
    (tmp_path / "a.sysml").write_text("package A;")
    (tmp_path / "a.json").write_text("{}")
    assert digest_model(tmp_path, patterns=("*.json",)).files == 1


def test_empty_directory_is_absent(tmp_path: Path):
    assert digest_model(tmp_path).digest == ABSENT
