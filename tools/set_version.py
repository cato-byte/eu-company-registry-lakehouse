"""Read or stamp the package version.

    python tools/set_version.py                   # print the version, e.g. 1.0.0
    python tools/set_version.py +57.g1a2b3c4      # keep the release, add a build tag: 1.0.0+57.g1a2b3c4
    python tools/set_version.py 1.1.0             # set a new release version

CD stamps a build tag before every deploy, never committed: each deployed wheel gets a unique file
name, and the name still says which release it was built from.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

INIT = Path(__file__).resolve().parents[1] / "src" / "registry_lakehouse" / "__init__.py"
VERSION = re.compile(r'__version__ = "([^"]+)"')
RELEASE = r"\d+\.\d+\.\d+"
LOCAL = r"\+[0-9A-Za-z.]+"


def current() -> str:
    match = VERSION.search(INIT.read_text())
    if not match:
        raise SystemExit(f"no __version__ in {INIT}")
    return match.group(1)


def main(argv: list[str]) -> int:
    if not argv:
        print(current())
        return 0
    if len(argv) != 1 or not re.fullmatch(f"({RELEASE})?({LOCAL})?", argv[0]) or not argv[0]:
        print("usage: set_version.py [MAJOR.MINOR.PATCH][+local]", file=sys.stderr)
        return 2
    new = argv[0]
    if new.startswith("+"):
        new = current().split("+")[0] + new
    INIT.write_text(VERSION.sub(f'__version__ = "{new}"', INIT.read_text()))
    print(f"version -> {new}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
