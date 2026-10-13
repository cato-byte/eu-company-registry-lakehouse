"""Guards that keep real or sensitive data out of the public repository.

1. Nothing in the tracked tree contains infrastructure identifiers (bucket names, AWS account
   ids, access keys) or real e-mail addresses.
2. When the private samples exist locally (data/samples, git-ignored), no real identifier or
   name from them appears anywhere in the committed fixtures. CI has no samples, so there
   only check 1 runs.
"""

from __future__ import annotations

import csv
import io
import re
import subprocess

import pytest

from tests.conftest import FIXTURES, ROOT
from tools.check_sensitive import problems, read_text

SAMPLES = ROOT / "data" / "samples"

def tracked_files() -> list:
    try:
        out = subprocess.run(["git", "ls-files", "-co", "--exclude-standard"], cwd=ROOT, capture_output=True,
                             text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("git not available")
    return [ROOT / p for p in out.splitlines() if (ROOT / p).is_file()]


read_any = read_text


def test_no_infrastructure_identifiers_secrets_or_emails_in_repo():
    hits = [p for f in tracked_files() for p in problems(f, str(f.relative_to(ROOT)))]
    assert not hits, "\n".join(hits)


def test_data_dir_is_ignored_by_git():
    out = subprocess.run(["git", "check-ignore", "data/samples/x.csv", ".env", "data/reference/x.txt"], cwd=ROOT,
                         capture_output=True, text=True)
    assert len(out.stdout.splitlines()) == 3


# ------------------------------------------------------------------------------------------
SPECS = {  # sample file -> (fixture glob, delimiter, id columns, name columns)
    "ESTONIA.part": ("ESTONIA/*/*.csv", ";", ["ariregistri_kood"], ["nimi"]),
    "IRELAND.part": ("IRELAND/*/*.csv", ",", ["company_num"], ["company_name"]),
    "SPAIN.part": ("SPAIN/*/*", ",", ["dnici"], ["nombre"]),
    "UK.head.part": ("UNITED_KINGDOM/*/*.csv", ",", ["CompanyNumber"], ["CompanyName"]),
    "FRANCE.head.part": ("FRANCE/*/*.csv", ",", ["siren", "siret"],
                         ["denominationUsuelleEtablissement", "enseigne1Etablissement"]),
}


def _columns(text: str, delim: str, id_cols: list[str], name_cols: list[str]) -> tuple[set[str], set[str]]:
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)
    reader.fieldnames = [c.strip() for c in reader.fieldnames or []]
    ids, names = set(), set()
    for row in reader:
        ids.update(v for c in id_cols if (v := (row.get(c) or "").strip()))
        names.update(v for c in name_cols if (v := (row.get(c) or "").strip()) and len(v) >= 8)
    return ids, names


@pytest.mark.skipif(not SAMPLES.exists(), reason="private samples not present (CI)")
@pytest.mark.parametrize("sample", sorted(SPECS))
def test_fixtures_contain_no_real_identifier_or_name(sample):
    pattern, delim, id_cols, name_cols = SPECS[sample]
    text = (SAMPLES / sample).read_text(encoding="utf-8-sig", errors="ignore")
    real_ids, real_names = _columns(text[: text.rfind("\n")], delim, id_cols, name_cols)
    fixture_ids, fixture_names = set(), set()
    for f in (FIXTURES / "landing").glob(pattern):
        ids, names = _columns(f.read_text(encoding="utf-8-sig"), delim, id_cols, name_cols)
        fixture_ids |= ids
        fixture_names |= names
    assert fixture_ids, pattern
    assert not fixture_ids & real_ids, f"real identifiers in fixtures: {sorted(fixture_ids & real_ids)[:10]}"
    assert not fixture_names & real_names, f"real names in fixtures: {sorted(fixture_names & real_names)[:10]}"


@pytest.mark.skipif(not SAMPLES.exists(), reason="private samples not present (CI)")
def test_norway_fixture_contains_no_real_orgnr_or_name():
    import json

    sample = (SAMPLES / "NORWAY.json.part").read_text(encoding="utf-8", errors="ignore")[:50_000_000]
    real_ids = set(re.findall(r'"organisasjonsnummer" : "(\d{9})"', sample))
    real_names = set(re.findall(r'"navn" : "([^"]{8,})"', sample))
    records = [json.loads(line) for f in (FIXTURES / "landing" / "NORWAY").rglob("*.gz")
               for line in read_any(f).splitlines() if line]
    assert records
    assert not {r["organisasjonsnummer"] for r in records} & real_ids
    assert not {r["navn"] for r in records} & real_names
