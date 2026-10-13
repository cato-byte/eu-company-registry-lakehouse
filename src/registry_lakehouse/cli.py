"""Single entrypoint for every pipeline task (Databricks python_wheel_task or local).

    registry-lakehouse bronze  --country FR
    registry-lakehouse silver  --country FR
    registry-lakehouse reference
    registry-lakehouse geo
    registry-lakehouse gold
    registry-lakehouse publish [--samples]
    registry-lakehouse cost-report                 # real DBUs per run from system tables (Databricks only)
    registry-lakehouse all     [--samples]        # whole pipeline, e.g. locally on fixtures

Every task fails loudly (non-zero exit), so Databricks job notifications fire. Non-fatal findings
go through ops.alerts.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date

from registry_lakehouse.config import Settings, settings_from_env
from registry_lakehouse.contracts import COUNTRY_CODES
from registry_lakehouse.ops.alerts import notify

log = logging.getLogger("registry_lakehouse")


def _settings(args: argparse.Namespace) -> Settings:
    s = settings_from_env()
    return s.with_overrides(
        landing_root=args.landing_root.rstrip("/") if args.landing_root else None,
        catalog=args.catalog,
        publish_root=args.publish_root,
        snapshot_date=date.fromisoformat(args.snapshot_date) if args.snapshot_date else None,
        run_id=args.run_id,
    )


def run_bronze(spark, settings: Settings, countries: list[str]) -> None:
    from registry_lakehouse.bronze.load import load_bronze

    for cc in countries:
        res = load_bronze(spark, settings, cc)
        if res.drift.has_drift:
            notify("WARN", f"schema drift in {cc} (non-breaking)", res.drift.summary())
        if res.corrupt_rows:
            notify("WARN", f"{res.corrupt_rows:,} malformed lines in {cc} bronze",
                   "Kept in _corrupt_record and excluded from silver.")


def run_silver(spark, settings: Settings, countries: list[str]) -> None:
    from registry_lakehouse.silver.build import build_silver

    for cc in countries:
        for res in build_silver(spark, settings, cc):
            if res.quarantined:
                notify("INFO", f"{res.quarantined:,} rows quarantined for {cc} {res.snapshot_date}")


def run_reference(spark, settings: Settings) -> None:
    from registry_lakehouse.geo.reference import load_place_reference, load_postal_reference

    log.info("postal reference rows: %s", load_postal_reference(spark, settings))
    log.info("town reference rows: %s", load_place_reference(spark, settings))


def run_geo(spark, settings: Settings, batch: int) -> None:
    from registry_lakehouse.geo.geocode import build_locations

    build_locations(spark, settings, request_batch_size=batch)


def run_gold(spark, settings: Settings) -> None:
    from registry_lakehouse.gold.build import build_gold

    build_gold(spark, settings)


def run_publish(spark, settings: Settings, samples: bool) -> None:
    from registry_lakehouse.publish.static_export import publish

    publish(spark, settings, include_samples=samples)


def run_cost_report(spark, settings: Settings) -> None:
    from registry_lakehouse.ops.cost import REPORT_SQL
    from registry_lakehouse.spark import ensure_schema

    try:
        report = spark.sql(REPORT_SQL)
    except Exception as exc:  # system tables are a Databricks/Unity Catalog feature
        log.warning("cost report skipped (system tables unavailable): %s", str(exc).splitlines()[0])
        return
    ensure_schema(spark, settings.catalog, settings.ops_schema)
    report.write.format("delta").mode("overwrite").saveAsTable(settings.table("ops", "cost_by_run"))


def load_secrets(scope: str | None) -> None:
    """Expose Databricks secrets as env vars for the modules that need them."""
    import os

    from registry_lakehouse.spark import on_databricks

    if not scope or not on_databricks():
        return
    os.environ["REGISTRY_SECRET_SCOPE"] = scope
    from databricks.sdk.runtime import dbutils  # type: ignore[import-not-found]

    os.environ.setdefault("REGISTRY_PSEUDONYM_KEY", dbutils.secrets.get(scope=scope, key="pseudonym-key"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="registry-lakehouse")
    ap.add_argument("stage", choices=["bronze", "silver", "reference", "geo", "gold", "publish", "cost-report", "all"])
    ap.add_argument("--country", action="append", choices=COUNTRY_CODES,
                    help="repeatable. Default: all countries")
    ap.add_argument("--landing-root")
    ap.add_argument("--catalog")
    ap.add_argument("--publish-root")
    ap.add_argument("--snapshot-date")
    ap.add_argument("--run-id")
    ap.add_argument("--geocode-batch", type=int, default=1000)
    ap.add_argument("--samples", action="store_true", help="export stage samples (synthetic data only!)")
    ap.add_argument("--secret-scope", default=None, help="Databricks secret scope (pseudonym-key, alert-webhook)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from registry_lakehouse.spark import get_spark

    load_secrets(args.secret_scope)
    spark = get_spark()
    settings = _settings(args)
    countries = args.country or list(COUNTRY_CODES)
    started = time.monotonic()
    try:
        if args.stage in ("bronze", "all"):
            run_bronze(spark, settings, countries)
        if args.stage in ("silver", "all"):
            run_silver(spark, settings, countries)
        if args.stage in ("reference", "all"):
            run_reference(spark, settings)
        if args.stage in ("geo", "all"):
            run_geo(spark, settings, args.geocode_batch)
        if args.stage in ("gold", "all"):
            run_gold(spark, settings)
        if args.stage in ("publish", "all"):
            run_publish(spark, settings, args.samples)
        if args.stage == "cost-report":
            run_cost_report(spark, settings)
    except Exception as exc:
        notify("ERROR", f"stage '{args.stage}' failed for {countries}", f"{type(exc).__name__}: {exc}")
        raise
    log.info("stage %s done in %.1fs", args.stage, time.monotonic() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
