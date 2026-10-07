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


def test_file_vanishing_mid_snapshot_is_a_changed_snapshot(tmp_path: Path, monkeypatch):
    from airflow_provider_opensysml import digest

    (tmp_path / "a.sysml").write_text("package A;")
    (tmp_path / "b.sysml").write_text("package B;")
    only_a = digest_model(tmp_path, ["a.sysml"])

    real = digest.model_files
    calls = 0

    def enumerate_then_remove(path, patterns):
        nonlocal calls
        calls += 1
        files = real(path, patterns)
        if calls == 1:
            (tmp_path / "b.sysml").unlink()
        return files

    monkeypatch.setattr(digest, "model_files", enumerate_then_remove)
    assert digest_model(tmp_path) == only_a
    assert calls == 2


def test_file_that_keeps_vanishing_raises(tmp_path: Path, monkeypatch):
    import pytest

    from airflow_provider_opensysml import digest

    (tmp_path / "a.sysml").write_text("package A;")
    monkeypatch.setattr(digest, "model_files", lambda path, patterns: [tmp_path / "gone.sysml"])
    with pytest.raises(FileNotFoundError):
        digest_model(tmp_path)
