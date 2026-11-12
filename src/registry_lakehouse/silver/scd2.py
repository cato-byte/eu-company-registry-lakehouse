"""SCD Type 2 load of full register snapshots into silver.company with one Delta MERGE.

For a country C and snapshot S:
  * new company                  -> insert version (valid_from = S, is_current)
  * tracked attributes changed   -> close current version (valid_to = S) + insert new version
  * unchanged                    -> nothing written
  * missing from the snapshot    -> close current version (it left the register)

Every snapshot is a complete register, so "missing" really means gone. The ON clause is scoped to
``country_code = C``, so other countries' rows are pruned (the table is liquid-clustered on
country_code) and never touched.

Which snapshots were processed is kept apart from what changed: after each successful MERGE the
snapshot date is appended to a watermark table (``ops.silver_snapshots``). ``MAX(valid_from)`` cannot
serve as that watermark, because it only moves when some company changes: a snapshot with no
changes, or only removals, would otherwise be parsed again on every run. The watermark is written
right after the MERGE; if a run dies in between, the next run merges the same snapshot again,
which writes nothing new, so the watermark can lag but never run ahead of the data.
"""

from __future__ import annotations

import logging
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from registry_lakehouse.silver.canonical import CANONICAL_ORDER, silver_ddl
from registry_lakehouse.spark import create_table_if_missing

log = logging.getLogger(__name__)

SCD_COLUMNS = ["valid_from", "valid_to", "is_current", "_run_id", "_updated_at"]


class SnapshotOrderError(RuntimeError):
    pass


def ensure_company_table(spark: SparkSession, table: str) -> None:
    create_table_if_missing(
        spark,
        table,
        silver_ddl() + ", valid_from DATE NOT NULL, valid_to DATE, is_current BOOLEAN NOT NULL, "
        "_run_id STRING, _updated_at TIMESTAMP",
        cluster_by=["country_code", "nace_division"],
        comment="Silver: harmonised companies/establishments of six EU registers, SCD Type 2",
        extra_properties={
            "delta.dataSkippingStatsColumns":
                "country_code,nace_division,nace_section,status,company_key,valid_from,valid_to",
            # Row-level changes per snapshot (new, changed, closed companies) for incremental readers.
            "delta.enableChangeDataFeed": "true",
        },
        constraints={
            "valid_country": "country_code IN ('FR','GB','IE','EE','ES','NO')",
            "valid_status": "status IN ('ACTIVE','LIQUIDATION','INSOLVENCY','CLOSED','UNKNOWN')",
            "valid_interval": "valid_to IS NULL OR valid_to >= valid_from",
            "current_is_open": "NOT is_current OR valid_to IS NULL",
            "nace_format": "nace_code IS NULL OR nace_code RLIKE '^[0-9]{2}\\.[0-9]{2}$'",
            "company_key_present": "company_key IS NOT NULL",
        },
    )


def ensure_watermark_table(spark: SparkSession, table: str) -> None:
    create_table_if_missing(
        spark, table,
        "country_code STRING NOT NULL, snapshot_date DATE NOT NULL, _run_id STRING, _loaded_at TIMESTAMP",
        cluster_by=["country_code"],
        comment="Silver: every register snapshot merged into silver.company, per country (append-only "
                "watermark of what was processed, independent of whether anything changed)",
    )


def record_loaded_snapshot(spark: SparkSession, table: str, country: str, snapshot_date: date, run_id: str) -> None:
    # Blind append: parallel country tasks never conflict with each other.
    (
        spark.createDataFrame([(country, snapshot_date, run_id)],
                              "country_code string, snapshot_date date, _run_id string")
        .withColumn("_loaded_at", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(table)
    )


def forget_loaded_snapshots(spark: SparkSession, table: str, country: str) -> None:
    """Drop the country's watermark, so every snapshot is reloaded (business key rebuild)."""
    ensure_watermark_table(spark, table)
    spark.sql(f"DELETE FROM {table} WHERE country_code = '{country}'")


def last_loaded_snapshot(spark: SparkSession, table: str, watermark_table: str, country: str) -> date | None:
    """Newest snapshot merged for ``country``.

    Tables written before the watermark existed have no rows for the country: ``MAX(valid_from)`` is
    then used, a lower bound of the truth (the old behaviour), until the next merge records one.
    """
    ensure_watermark_table(spark, watermark_table)
    row = (spark.table(watermark_table).where(F.col("country_code") == country)
           .agg(F.max("snapshot_date").alias("m")).first())
    if row and row.m is not None:
        return row.m
    row = spark.table(table).where(F.col("country_code") == country).agg(F.max("valid_from").alias("m")).first()
    return row.m if row else None


def apply_snapshot(
    spark: SparkSession, table: str, watermark_table: str, snapshot: DataFrame, country: str,
    snapshot_date: date, run_id: str,
) -> dict[str, int]:
    """Merge one deduplicated snapshot (one row per company_key) into the SCD2 table."""
    last = last_loaded_snapshot(spark, table, watermark_table, country)
    if last is not None and snapshot_date < last:
        raise SnapshotOrderError(
            f"[{country}] snapshot {snapshot_date} is older than the last loaded one ({last}). "
            f"Restore the table to an earlier version (Delta RESTORE) and delete the newer rows of "
            f"{watermark_table} for {country} to reload history."
        )
    if last is not None and snapshot_date == last:
        log.info("[%s] snapshot %s already loaded: skipping (idempotent re-run)", country, snapshot_date)
        return {"inserted": 0, "closed": 0, "skipped": 1}

    src = snapshot.select(*CANONICAL_ORDER)
    target = DeltaTable.forName(spark, table)
    current = spark.table(table).where((F.col("country_code") == country) & F.col("is_current"))

    # Rows whose tracked attributes changed need two actions (close + insert). The classic
    # trick is to stage them twice: once with a NULL merge key so they are always inserted.
    changed = (
        src.alias("s")
        .join(current.select("company_key", "hash_diff").alias("t"), "company_key")
        .where(F.col("s.hash_diff") != F.col("t.hash_diff"))
        .select("s.*")
    )
    staged = src.withColumn("_merge_key", F.col("company_key")).unionByName(
        changed.withColumn("_merge_key", F.lit(None).cast("string"))
    )

    insert_values = {c: F.col(f"s.{c}") for c in CANONICAL_ORDER}
    insert_values.update({
        "valid_from": F.lit(snapshot_date), "valid_to": F.lit(None).cast("date"), "is_current": F.lit(True),
        "_run_id": F.lit(run_id), "_updated_at": F.current_timestamp(),
    })
    close = {"valid_to": F.lit(snapshot_date), "is_current": F.lit(False),
             "_run_id": F.lit(run_id), "_updated_at": F.current_timestamp()}

    (
        target.alias("t")
        .merge(staged.alias("s"),
               f"t.country_code = '{country}' AND t.is_current AND t.company_key = s._merge_key")
        .whenMatchedUpdate(condition="t.hash_diff <> s.hash_diff", set=close)
        .whenNotMatchedInsert(values=insert_values)  # new companies + new versions of changed ones
        .whenNotMatchedBySourceUpdate(condition=f"t.country_code = '{country}' AND t.is_current", set=close)
        .execute()
    )
    metrics = _last_merge_metrics(target)
    record_loaded_snapshot(spark, watermark_table, country, snapshot_date, run_id)
    log.info("[%s] SCD2 merge %s: %s", country, snapshot_date, metrics)
    return metrics


def _last_merge_metrics(target: DeltaTable) -> dict[str, int]:
    # Not history(1): predictive optimization may commit an OPTIMIZE right after the MERGE.
    row = (target.history(10).where("operation = 'MERGE'").orderBy(F.col("version").desc())
           .select("operationMetrics").first())
    m = row.operationMetrics if row else {}
    return {
        "inserted": int(m.get("numTargetRowsInserted", 0)),
        "closed": int(m.get("numTargetRowsUpdated", 0)),
        "skipped": 0,
    }
