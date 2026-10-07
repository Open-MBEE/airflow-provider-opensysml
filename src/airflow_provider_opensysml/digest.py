"""A content digest over the files of a SysML model.

The digest is what the watcher compares between polls. It reads file contents
rather than modification times, so a checkout that rewrites identical files does
not count as a change, and it covers every model file under a directory so a
multi-file model is one asset.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PATTERNS: tuple[str, ...] = ("*.sysml", "*.kerml")

ABSENT = "absent"


@dataclass(frozen=True)
class ModelDigest:
    """The digest of a model's files at one moment.

    ``digest`` is ``sha256:<hex>`` over the files' relative paths and contents,
    or ``absent`` when the path names nothing. ``files`` counts the files read.
    """

    digest: str
    files: int

    @property
    def present(self) -> bool:
        return self.digest != ABSENT


def model_files(path: str | Path, patterns: Sequence[str] = DEFAULT_PATTERNS) -> list[Path]:
    """The files the digest reads: the file ``path`` names, or every file under it matching ``patterns``."""
    root = Path(path)
    if root.is_file():
        return [root]
    if not root.is_dir():
        return []
    found: set[Path] = set()
    for pattern in patterns:
        found.update(f for f in root.rglob(pattern) if f.is_file())
    return sorted(found, key=lambda f: f.relative_to(root).as_posix())


def digest_model(path: str | Path, patterns: Sequence[str] = DEFAULT_PATTERNS) -> ModelDigest:
    """Digest the model at ``path``."""
    root = Path(path)
    files = model_files(root, patterns)
    if not files:
        return ModelDigest(ABSENT, 0)
    base = root if root.is_dir() else root.parent
    return ModelDigest(_digest(base, files), len(files))


def _digest(base: Path, files: Iterable[Path]) -> str:
    h = hashlib.sha256()
    for f in files:
        content = f.read_bytes()
        h.update(f.relative_to(base).as_posix().encode("utf-8"))
        h.update(b"\0")
        h.update(len(content).to_bytes(8, "big"))
        h.update(content)
    return "sha256:" + h.hexdigest()
