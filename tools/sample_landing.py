"""Download the first bytes of every landing file into data/samples/ (git-ignored).

Range requests only, so no multi-GB transfers. The samples stay on your machine. Run
tools/make_fixtures.py next to derive the synthetic, committable test fixtures from them.

    REGISTRY_LANDING_S3=s3://<bucket>/company_records/lakehouse/landing python tools/sample_landing.py
"""

from __future__ import annotations

import os
import sys

import boto3

SAMPLES = os.path.join(os.path.dirname(__file__), "..", "data", "samples")
# (country folder, file name, byte ranges, local name). A mid-file range adds variety.
PLAN = [
    ("ESTONIA", "ettevotja_rekvisiidid__lihtandmed.csv", [(0, 20_000_000)], "ESTONIA.part"),
    ("FRANCE", "StockEtablissement_utf8.csv", [(0, 15_000_000)], "FRANCE.head.part"),
    ("FRANCE", "StockEtablissement_utf8.csv", [(5_000_000_000, 5_030_000_000)], "FRANCE.mid.part"),
    ("IRELAND", "companies.csv", [(0, 30_000_000)], "IRELAND.part"),
    ("SPAIN", "0c4b4747-026f-479a-9966-6568a88957f5", [(0, 72_000_000)], "SPAIN.part"),
    ("UNITED_KINGDOM", "BasicCompanyDataAsOneFile-2026-07-01.csv", [(0, 10_000_000)], "UK.head.part"),
    ("UNITED_KINGDOM", "BasicCompanyDataAsOneFile-2026-07-01.csv", [(1_200_000_000, 1_230_000_000)], "UK.mid.part"),
    ("NORWAY", "norway_company_records_json.gz", [(0, 25_000_000)], "NORWAY.gz.part"),
]


def main() -> int:
    root = os.environ.get("REGISTRY_LANDING_S3")
    if not root or not root.startswith("s3://"):
        print("set REGISTRY_LANDING_S3=s3://<bucket>/<prefix>/landing", file=sys.stderr)
        return 2
    bucket, _, prefix = root[5:].partition("/")
    os.makedirs(SAMPLES, exist_ok=True)
    s3 = boto3.client("s3")
    for folder, name, ranges, local in PLAN:
        for start, end in ranges:
            obj = s3.get_object(Bucket=bucket, Key=f"{prefix}/{folder}/{name}", Range=f"bytes={start}-{end}")
            with open(os.path.join(SAMPLES, local), "wb") as fh:
                fh.write(obj["Body"].read())
        print("sampled", folder, local)
    return 0


if __name__ == "__main__":
    sys.exit(main())
