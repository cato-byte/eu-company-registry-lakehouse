"""Print the CHANGELOG.md section of one release, for the GitHub release body.

    python tools/release_notes.py 1.0.0

Exits non-zero when the section is missing or empty, so a tag cannot be released without notes.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

CHANGELOG = Path(__file__).resolve().parents[1] / "CHANGELOG.md"


def section(text: str, version: str) -> str:
    """The body under `## [version]`, up to the next `## ` heading or the link definitions."""
    heading = re.compile(rf"^## \[{re.escape(version)}\][^\n]*\n", re.MULTILINE)
    match = heading.search(text)
    if not match:
        return ""
    rest = text[match.end():]
    end = re.search(r"^(## |\[[^\]]+\]: )", rest, re.MULTILINE)
    return (rest[: end.start()] if end else rest).strip()


def main(argv: list[str]) -> int:
    if len(argv) != 1:
        print("usage: release_notes.py VERSION", file=sys.stderr)
        return 2
    notes = section(CHANGELOG.read_text(), argv[0].removeprefix("v"))
    if not notes:
        print(f"CHANGELOG.md has no notes for {argv[0]}", file=sys.stderr)
        return 1
    print(notes)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
