"""Smoke-test an installed wheel: run with the Python of a clean virtualenv, outside the checkout.

    python -m venv /tmp/wheel-venv && /tmp/wheel-venv/bin/pip install dist/*.whl
    cd /tmp && /tmp/wheel-venv/bin/python <repo>/tools/wheel_smoke.py

Tests run against an editable install, which reads data files straight from ``src/``. This checks
the artifact that Databricks actually receives: the package imports from site-packages, the
packaged data files load, and both console scripts start. Only the wheel's own dependencies are
installed, as on serverless, which provides pyspark itself.
"""

from __future__ import annotations

import csv
import shutil
import subprocess
import sys
from importlib import metadata, resources
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def check(condition: bool, message: str) -> None:
    if not condition:
        sys.exit(f"wheel smoke test FAILED: {message}")
    print(f"ok  {message}")


def main() -> int:
    import registry_lakehouse
    from registry_lakehouse.contracts import COUNTRY_CODES, load_contracts

    location = Path(registry_lakehouse.__file__).resolve()
    check(REPO not in location.parents, f"package imported from the installed wheel ({location.parent})")
    check(metadata.version("registry-lakehouse") == registry_lakehouse.__version__,
          f"installed version is {registry_lakehouse.__version__}")

    contracts = load_contracts()
    check(set(contracts) == set(COUNTRY_CODES) and len(contracts) == 6,
          f"contracts/sources.yml packaged ({len(contracts)} registers)")

    nace = resources.files("registry_lakehouse.reference").joinpath("nace_rev2_divisions.csv")
    check(nace.is_file(), "reference/nace_rev2_divisions.csv is in the wheel")
    rows = list(csv.DictReader(nace.read_text("utf-8").splitlines()))
    divisions = {r["division"]: r["section"] for r in rows}
    check(len(divisions) > 80 and divisions.get("03") == "A",
          f"reference/nace_rev2_divisions.csv packaged ({len(divisions)} divisions)")

    bin_dir = Path(sys.executable).parent
    for args in (["registry-lakehouse", "--help"], ["registry-cost", "estimate", "--sizes", "FR=1GB,GB=1GB"]):
        exe = shutil.which(args[0], path=str(bin_dir))
        check(exe is not None, f"console script {args[0]} installed")
        result = subprocess.run([exe, *args[1:]], capture_output=True, text=True, check=False)
        check(result.returncode == 0, f"`{' '.join(args)}` exits 0" + (f"\n{result.stderr}" if result.returncode else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
