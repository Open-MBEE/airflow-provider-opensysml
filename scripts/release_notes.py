"""Print the CHANGELOG.md section of one version, failing when it has none."""

from __future__ import annotations

import re
import sys
from pathlib import Path


def section(changelog: str, version: str) -> str:
    match = re.search(rf"^## \[?{re.escape(version)}\]?(?: .*)?$", changelog, flags=re.MULTILINE)
    if match is None:
        raise SystemExit(f"CHANGELOG.md has no section for {version}; move Unreleased under it first")
    rest = changelog[match.end() :]
    following = re.search(r"^## ", rest, flags=re.MULTILINE)
    return rest[: following.start() if following else len(rest)].strip() + "\n"


if __name__ == "__main__":
    sys.stdout.write(section(Path("CHANGELOG.md").read_text(), sys.argv[1]))
