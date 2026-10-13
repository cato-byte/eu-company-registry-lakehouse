"""Ad hoc cost estimator for Databricks serverless jobs.

Serverless bills DBUs for the compute a run actually uses, and nothing in Databricks predicts
that before a run. This estimator works the way capacity planners do: cost scales with the
bytes each stage reads, times a per-stage DBU/GB coefficient learned from past runs.

  1. ``registry-cost report-sql``  prints the system-table query that gives real DBUs per run
     (system.billing.usage joined with system.billing.list_prices).
  2. ``registry-cost calibrate``   fits DBU/GB per stage from that history (CSV export).
  3. ``registry-cost estimate``    predicts DBUs and USD for the current landing sizes, with a
     range. Before calibration it uses conservative priors and says so.

The DBU price varies by cloud, region, plan and contract: pass ``--usd-per-dbu`` from your own
price list (system.billing.list_prices also has it).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass

# Uncalibrated priors (DBU per GB of landing input per stage). They are deliberately high:
# an estimator that under-predicts is worse than none. Replace them with `calibrate`.
PRIOR_DBU_PER_GB = {"bronze": 0.30, "silver": 0.60, "geo": 0.15, "gold": 0.10, "publish": 0.02}
# Relative parse cost of source formats (JSON parsing is heavier than CSV splitting).
FORMAT_FACTOR = {"csv": 1.0, "ndjson": 1.6}
FIXED_DBU_PER_TASK = 0.02  # start-up and small-table overhead per task
DEFAULT_USD_PER_DBU = 0.35  # placeholder: check your own list price

REPORT_SQL = """
-- Real DBUs and list cost per job run and task (Unity Catalog system tables).
WITH usage AS (
  SELECT
    usage_metadata.job_id      AS job_id,
    usage_metadata.job_run_id  AS run_id,
    usage_metadata.job_name    AS job_name,
    usage_date,
    sku_name,
    usage_quantity             AS dbus,
    usage_start_time
  FROM system.billing.usage
  WHERE usage_metadata.job_id IS NOT NULL
    AND product_features.is_serverless
    AND usage_date >= current_date() - INTERVAL 90 DAYS
)
SELECT u.job_name, u.job_id, u.run_id, MIN(u.usage_start_time) AS started,
       SUM(u.dbus) AS dbus,
       SUM(u.dbus * p.pricing.effective_list.default) AS list_cost_usd
FROM usage u
LEFT JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.job_name LIKE '%registry%'
GROUP BY ALL
ORDER BY started DESC
""".strip()


@dataclass(frozen=True)
class Estimate:
    stage: str
    country: str
    input_gb: float
    dbu: float

    def usd(self, price: float) -> float:
        return self.dbu * price


def parse_size(text: str) -> float:
    """'9.3GB' / '200MB' / '1.2e9' -> GB."""
    m = re.fullmatch(r"\s*([\d.eE+]+)\s*([KMGT]?B)?\s*", text, flags=re.I)
    if not m:
        raise ValueError(f"bad size: {text}")
    value = float(m.group(1))
    unit = (m.group(2) or "B").upper()
    return value * {"B": 1 / 1024**3, "KB": 1 / 1024**2, "MB": 1 / 1024, "GB": 1.0, "TB": 1024.0}[unit]


def estimate(sizes_gb: dict[str, float], coefficients: dict[str, float] | None = None,
             formats: dict[str, str] | None = None) -> list[Estimate]:
    coef = coefficients or PRIOR_DBU_PER_GB
    fmt = formats or {"NO": "ndjson"}
    out = []
    for country, gb in sorted(sizes_gb.items()):
        factor = FORMAT_FACTOR.get(fmt.get(country, "csv"), 1.0)
        for stage in ("bronze", "silver"):
            out.append(Estimate(stage, country, gb, FIXED_DBU_PER_TASK + coef[stage] * gb * factor))
    total_gb = sum(sizes_gb.values())
    for stage in ("geo", "gold", "publish"):
        out.append(Estimate(stage, "ALL", total_gb, FIXED_DBU_PER_TASK + coef[stage] * total_gb))
    return out


def calibrate(history_csv: str) -> dict[str, float]:
    """Least-squares through the origin per stage: DBU ~ k * GB. Columns: stage,input_gb,dbus."""
    sums: dict[str, list[float]] = {}
    with open(history_csv, newline="") as fh:
        for row in csv.DictReader(fh):
            gb, dbu = float(row["input_gb"]), float(row["dbus"])
            s = sums.setdefault(row["stage"], [0.0, 0.0])
            s[0] += gb * dbu
            s[1] += gb * gb
    return {stage: (xy / xx if xx else PRIOR_DBU_PER_GB.get(stage, 0.0)) for stage, (xy, xx) in sums.items()}


def landing_sizes_from_s3(landing_root: str) -> dict[str, float]:
    import boto3

    from registry_lakehouse.contracts import load_contracts

    bucket, _, prefix = landing_root[5:].partition("/")
    s3 = boto3.client("s3")
    sizes = {}
    for code, contract in load_contracts().items():
        total = 0
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/{contract.country}/"):
            total += sum(o["Size"] for o in page.get("Contents", []))
        sizes[code] = total / 1024**3
    return sizes


def render(estimates: list[Estimate], price: float, calibrated: bool) -> str:
    lines = [f"{'stage':8} {'country':7} {'input GB':>9} {'DBU':>8} {'USD':>8}"]
    for e in estimates:
        lines.append(f"{e.stage:8} {e.country:7} {e.input_gb:9.2f} {e.dbu:8.2f} {e.usd(price):8.2f}")
    total = sum(e.dbu for e in estimates)
    # A wider band before calibration: the priors are guesses.
    lo, hi = (0.7, 1.4) if calibrated else (0.4, 2.0)
    lines.append(f"TOTAL{'':20} {total:8.2f} {total * price:8.2f}")
    lines.append(f"range: ${total * price * lo:.2f} - ${total * price * hi:.2f} per full run "
                 f"({'calibrated' if calibrated else 'UNCALIBRATED priors: run calibrate after a few runs'})")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="registry-cost", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("estimate", help="predict the cost of a full run")
    src = e.add_mutually_exclusive_group(required=True)
    src.add_argument("--sizes", help="e.g. FR=9.3GB,GB=2.6GB,NO=200MB")
    src.add_argument("--landing", help="s3://<bucket>/<prefix>/landing (lists object sizes)")
    e.add_argument("--coefficients", help="JSON from `calibrate`")
    e.add_argument("--usd-per-dbu", type=float, default=float(os.environ.get("REGISTRY_USD_PER_DBU",
                                                                             DEFAULT_USD_PER_DBU)))
    c = sub.add_parser("calibrate", help="fit DBU/GB per stage from run history")
    c.add_argument("history_csv")
    sub.add_parser("report-sql", help="print the system-tables query for real costs")
    args = ap.parse_args(argv)

    if args.cmd == "report-sql":
        print(REPORT_SQL)
        return 0
    if args.cmd == "calibrate":
        print(json.dumps(calibrate(args.history_csv), indent=2))
        return 0
    if args.sizes:
        sizes = {k.strip().upper(): parse_size(v) for k, v in (p.split("=") for p in args.sizes.split(","))}
    else:
        sizes = landing_sizes_from_s3(args.landing)
    coef = None
    if args.coefficients:
        with open(args.coefficients) as fh:
            coef = {**PRIOR_DBU_PER_GB, **json.load(fh)}
    print(render(estimate(sizes, coef), args.usd_per_dbu, calibrated=coef is not None))
    return 0


if __name__ == "__main__":
    sys.exit(main())

