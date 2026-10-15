"""Skip work when inputs have not changed: running on the same data gives the same tables.

Every stage computes a fingerprint of its inputs and compares it with the one recorded after its
last successful run (table ``ops.stage_state``):

* bronze      newest landing snapshot of the country: file paths and sizes
* reference   latest GeoNames files: paths and sizes
* geo         data versions of silver.company / reference tables + geocoding result files
* gold        data versions of silver.company, silver.location, silver.ref_place + insight settings
* publish     data versions of the gold tables it exports

A "data version" is the latest Delta version produced by a data-changing operation. Maintenance
commits (OPTIMIZE, VACUUM, property changes...) are ignored: predictive optimisation runs them at
any time, and they must not trigger a recomputation. Unknown operations count as data changes, so
a new kind of write is never silently skipped.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import time
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from registry_lakehouse.config import Settings
from registry_lakehouse.spark import create_table_if_missing, ensure_schema, table_exists

log = logging.getLogger(__name__)

MAINTENANCE_OPERATIONS = {
    "OPTIMIZE", "VACUUM START", "VACUUM END", "SET TBLPROPERTIES", "UNSET TBLPROPERTIES",
    "ADD CONSTRAINT", "DROP CONSTRAINT", "CHANGE COLUMN", "UPGRADE PROTOCOL", "REORG", "ANALYZE",
    "COMPUTE STATISTICS", "CLUSTER BY", "ROW TRACKING BACKFILL", "DOMAIN METADATA CLEANUP",
}


def fingerprint(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def files_fingerprint(files: list[Any]) -> str:
    """Order-independent fingerprint of (path, size) pairs. Accepts LandingFile-like objects."""
    return fingerprint(sorted((f.path, f.size_bytes) for f in files))


def _last_data_commit(spark: SparkSession, table: str) -> tuple[int, Any] | None:
    """(version, timestamp) of the latest data-changing commit, or None if the table is missing."""
    if not table_exists(spark, table):
        return None
    from delta.tables import DeltaTable

    # Delta API, not "DESCRIBE HISTORY <name>": that SQL misparses table names that are keywords
    # (our silver table is called `location`).
    history = DeltaTable.forName(spark, table).history().select("version", "timestamp", "operation")
    row = (history.where(~F.col("operation").isin(*MAINTENANCE_OPERATIONS))
           .orderBy(F.col("version").desc()).select("version", "timestamp").first())
    return (int(row.version), row.timestamp) if row else None


def data_version(spark: SparkSession, table: str) -> int:
    """Latest version of ``table`` written by a data-changing operation (-1 if the table is missing)."""
    commit = _last_data_commit(spark, table)
    return commit[0] if commit else -1


def outputs_newer_than_inputs(spark: SparkSession, outputs: list[str], inputs: list[str]) -> bool:
    """Make-style check: every output was last written after every input last changed."""
    out = [_last_data_commit(spark, t) for t in outputs]
    inp = [_last_data_commit(spark, t) for t in inputs]
    if not out or any(c is None for c in out) or any(c is None for c in inp):
        return False
    return min(c[1] for c in out) > max((c[1] for c in inp), default=min(c[1] for c in out))


def data_versions(spark: SparkSession, tables: list[str]) -> dict[str, int]:
    return {t: data_version(spark, t) for t in tables}


def _state_table(settings: Settings) -> str:
    return settings.table("ops", "stage_state")


def last_fingerprint(spark: SparkSession, settings: Settings, stage: str) -> str | None:
    table = _state_table(settings)
    if not table_exists(spark, table):
        return None
    row = spark.table(table).where(F.col("stage") == stage).select("fingerprint").first()
    return row.fingerprint if row else None


def unchanged(spark: SparkSession, settings: Settings, stage: str, fp: str,
              outputs: list[str] | None = None, inputs: list[str] | None = None) -> bool:
    """True when the stage can be skipped.

    With no recorded fingerprint (first run with change detection, or a lost state table), the
    stage is still skipped if its ``outputs`` are newer than all its ``inputs``: the result is then
    adopted and its fingerprint recorded, instead of paying for an identical recomputation.
    """
    last = last_fingerprint(spark, settings, stage)
    if last == fp:
        log.info("[%s] inputs unchanged since the last successful run: skipping", stage)
        return True
    if last is None and outputs and inputs is not None and outputs_newer_than_inputs(spark, outputs, inputs):
        log.info("[%s] no recorded run, but outputs are newer than inputs: adopting them", stage)
        record(spark, settings, stage, fp)
        return True
    return False


def record(spark: SparkSession, settings: Settings, stage: str, fp: str) -> None:
    """Remember the inputs of a successful stage run (one row per stage)."""
    ensure_schema(spark, settings.catalog, settings.ops_schema)
    table = _state_table(settings)
    create_table_if_missing(
        spark, table, "stage STRING NOT NULL, fingerprint STRING NOT NULL, run_id STRING, updated_at TIMESTAMP",
        comment="Fingerprint of the inputs of each stage at its last successful run (change detection)",
    )
    from delta.tables import DeltaTable

    src = spark.createDataFrame([(stage, fp, settings.run_id)], "stage string, fingerprint string, run_id string") \
        .withColumn("updated_at", F.current_timestamp())
    # Parallel tasks (one per country) record at the same time: retry on concurrent-commit conflicts.
    for attempt in range(6):
        try:
            (
                DeltaTable.forName(spark, table).alias("t")
                .merge(src.alias("s"), "t.stage = s.stage")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            return
        except Exception as exc:
            if "Concurrent" not in type(exc).__name__ + str(exc) or attempt == 5:
                raise
            time.sleep(2 + 3 * attempt + random.random())  # noqa: S311  # jitter, not crypto
