"""Stamp the package version before a deploy build.

    python tools/set_version.py 0.2.57+g1a2b3c4

Used by CD only, never committed: every deployed wheel gets a unique file name.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

INIT = Path(__file__).resolve().parents[1] / "src" / "registry_lakehouse" / "__init__.py"


def main(argv: list[str]) -> int:
    if len(argv) != 1 or not re.fullmatch(r"\d+\.\d+\.\d+(\+[0-9A-Za-z.]+)?", argv[0]):
        print("usage: set_version.py MAJOR.MINOR.PATCH[+local]", file=sys.stderr)
        return 2
    text = INIT.read_text()
    INIT.write_text(re.sub(r'__version__ = "[^"]+"', f'__version__ = "{argv[0]}"', text))
    print(f"version -> {argv[0]}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
