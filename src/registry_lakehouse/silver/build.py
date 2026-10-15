"""Bronze -> silver for one country: map, check quality, quarantine, deduplicate, SCD2 merge."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from registry_lakehouse.bronze.load import bronze_table_name
from registry_lakehouse.config import Settings
from registry_lakehouse.quality import engine
from registry_lakehouse.quality.rules import rules_for
from registry_lakehouse.silver.canonical import CANONICAL_ORDER, finalise
from registry_lakehouse.silver.scd2 import apply_snapshot, ensure_company_table, last_loaded_snapshot
from registry_lakehouse.silver.sources import MAPPERS
from registry_lakehouse.silver.sources.spain import snapshot_years
from registry_lakehouse.spark import create_table_if_missing, ensure_schema

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SilverResult:
    country_code: str
    snapshot_date: date
    rows_in: int
    quarantined: int
    duplicates_removed: int
    inserted: int
    closed: int
    skipped: bool
    dq_breaches: tuple[str, ...]


def canonical_snapshot(bronze: DataFrame, country: str, snapshot_date: date) -> DataFrame:
    """Pure transformation, unit-tested: bronze rows of one snapshot -> canonical rows."""
    m = MAPPERS[country]
    return finalise(m.mapper(bronze, snapshot_date), country, m.register, m.grain)


def deduplicate(df: DataFrame) -> DataFrame:
    """One row per company_key. Prefer the most complete row, and break ties on hash_diff so
    the result is deterministic (the same input always produces the same output)."""
    completeness = sum(
        (F.col(c).isNotNull().cast("int") for c in ("company_name", "address_line", "postal_code", "nace_code")),
        F.lit(0),
    )
    w = Window.partitionBy("company_key").orderBy(completeness.desc(), F.col("hash_diff"))
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def _pending_snapshots(spark: SparkSession, settings: Settings, country: str, bronze: DataFrame) -> list[date]:
    company_table = settings.table("silver", "company")
    last = last_loaded_snapshot(spark, company_table, country)
    if MAPPERS[country].embedded_snapshots:
        candidates = [date(y, 12, 31) for y in snapshot_years(bronze)]
    else:
        latest = bronze.agg(F.max("_snapshot_date").alias("d")).first()
        candidates = [latest.d] if latest and latest.d else []
    # Only snapshots newer than the last merged one: re-processing the same snapshot would parse
    # the whole register again just to find that nothing changed.
    pending = [d for d in candidates if last is None or d > last]
    if not pending:
        log.info("[%s] no snapshot newer than %s: skipping", country, last)
    return pending


def _bronze_for(bronze_all: DataFrame, country: str, snapshot: date) -> DataFrame:
    if MAPPERS[country].embedded_snapshots:
        latest = bronze_all.agg(F.max("_snapshot_date").alias("d")).first()
        return bronze_all.where(F.col("_snapshot_date") == F.lit(latest.d))
    return bronze_all.where(F.col("_snapshot_date") == F.lit(snapshot))


def ensure_quarantine_table(spark: SparkSession, table: str) -> None:
    create_table_if_missing(
        spark, table,
        "country_code STRING, snapshot_date DATE, company_key STRING, business_id STRING, "
        "failures ARRAY<STRING>, record STRING, _run_id STRING, _quarantined_at TIMESTAMP",
        cluster_by=["country_code", "snapshot_date"],
        comment="Rows that failed an ERROR-level data-quality rule, kept for inspection and replay",
    )


def rebuild_if_key_changed(spark: SparkSession, settings: Settings, country: str) -> bool:
    """If the country's business key changed since its rows were written, remove them so that all
    snapshots are reloaded with the new key. Returns True when a rebuild was triggered.

    Without a recorded key version (history written before versioning existed), version 1 is
    assumed: only countries whose key was bumped are rebuilt.
    """
    from registry_lakehouse.ops import freshness

    stage, wanted = f"silver_key:{country}", str(MAPPERS[country].key_version)
    recorded = freshness.last_fingerprint(spark, settings, stage) or "1"
    if recorded == wanted:
        if freshness.last_fingerprint(spark, settings, stage) is None:
            freshness.record(spark, settings, stage, wanted)
        return False
    log.warning("[%s] business key changed (v%s -> v%s): rebuilding silver history", country, recorded, wanted)
    for table in (settings.table("silver", "company"), settings.table("silver", "quarantine")):
        spark.sql(f"DELETE FROM {table} WHERE country_code = '{country}'")
    freshness.record(spark, settings, stage, wanted)
    return True


def build_silver(spark: SparkSession, settings: Settings, country: str) -> list[SilverResult]:
    ensure_schema(spark, settings.catalog, settings.silver_schema)
    ensure_schema(spark, settings.catalog, settings.ops_schema)
    company_table = settings.table("silver", "company")
    quarantine_table = settings.table("silver", "quarantine")
    dq_table = settings.table("ops", "dq_results")
    ensure_company_table(spark, company_table)
    ensure_quarantine_table(spark, quarantine_table)
    rebuild_if_key_changed(spark, settings, country)

    bronze_all = spark.table(bronze_table_name(settings, country)).where(F.col("_corrupt_record").isNull())
    results = []
    for snap in _pending_snapshots(spark, settings, country, bronze_all):
        canonical = canonical_snapshot(_bronze_for(bronze_all, country, snap), country, snap)
        rules = rules_for(country)
        # Materialise once: serverless has no .cache(), and DQ metrics, quarantine, counts and
        # the MERGE all read this result. Without staging, each would re-parse bronze.
        stage_table = settings.table("silver", "company_stage")
        (
            engine.annotate(canonical, rules).write.format("delta").mode("overwrite")
            .option("replaceWhere", f"country_code = '{country}'")
            .option("mergeSchema", "true")
            .saveAsTable(stage_table)
        )
        annotated = spark.table(stage_table).where(F.col("country_code") == country)
        previous = engine.previous_row_count(spark, dq_table, "silver.company", country, snap)
        outcome = engine.evaluate(
            annotated, rules, dataset="silver.company", country=country, snapshot_date=snap,
            run_id=settings.run_id, previous_total=previous, min_volume_ratio=settings.min_volume_ratio,
            min_rows_for_rate=settings.dq_min_rows_for_rate,
        )
        engine.write_metrics(spark, dq_table, outcome)  # persisted even when the run aborts
        if not outcome.ok:
            raise engine.DataQualityError(f"[{country} {snap}] data-quality thresholds breached: "
                                          + "; ".join(outcome.breaches))

        bad = annotated.where(F.col(engine.QUARANTINE_COL))
        (
            bad.select(
                "country_code", "snapshot_date", "company_key", "business_id",
                F.col(engine.FAILURES_COL).alias("failures"),
                F.to_json(F.struct(*CANONICAL_ORDER)).alias("record"),
                F.lit(settings.run_id).alias("_run_id"), F.current_timestamp().alias("_quarantined_at"),
            )
            .write.format("delta").mode("overwrite")
            .option("replaceWhere", f"country_code = '{country}' AND snapshot_date = DATE'{snap.isoformat()}'")
            .saveAsTable(quarantine_table)
        )
        good = annotated.where(~F.col(engine.QUARANTINE_COL)).drop(engine.FAILURES_COL, engine.QUARANTINE_COL)
        deduped = deduplicate(good)
        counts = (
            annotated.agg(
                F.count(F.lit(1)).alias("rows"),
                F.sum(F.col(engine.QUARANTINE_COL).cast("int")).alias("bad"),
                F.count_distinct(F.when(~F.col(engine.QUARANTINE_COL), F.col("company_key"))).alias("distinct_ok"),
            ).first()
        )
        rows_ok = counts.rows - (counts.bad or 0)
        merge = apply_snapshot(spark, company_table, deduped, country, snap, settings.run_id)
        results.append(SilverResult(
            country, snap, counts.rows, counts.bad or 0, rows_ok - counts.distinct_ok,
            merge["inserted"], merge["closed"], bool(merge["skipped"]), tuple(outcome.breaches),
        ))
        log.info("silver %s", results[-1])
    return results
