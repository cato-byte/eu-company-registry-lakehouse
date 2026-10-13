"""Landing -> bronze.

Bronze is a faithful, append-only copy of each register snapshot:

* every source column is a STRING. Nothing is parsed here, so a malformed date in row 40
  million cannot fail the load, and silver can always re-parse from the original text.
* columns are mapped by header *name*, after a drift check against the contract.
* PII columns listed in the contract are never read.
* unexpected extra columns are kept as JSON in ``_rescued_data``, and malformed lines in
  ``_corrupt_record``. Nothing is silently dropped.
* lineage: ``_source_file``, ``_snapshot_date``, ``_ingested_at``, ``_run_id``.
* idempotent: re-running a snapshot replaces that snapshot (``replaceWhere``).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from registry_lakehouse.bronze.landing import LandingSnapshot, list_landing_files, pick_snapshot
from registry_lakehouse.bronze.schema_drift import DriftReport, detect_drift, enforce, normalise_header
from registry_lakehouse.config import Settings
from registry_lakehouse.contracts import SourceContract, bronze_column_name, get_contract
from registry_lakehouse.spark import create_table_if_missing, ensure_schema, overwrite_partition

log = logging.getLogger(__name__)

CORRUPT_COL = "_corrupt_record"
METADATA_COLUMNS = ["_rescued_data", CORRUPT_COL, "_source_file", "_snapshot_date", "_ingested_at", "_run_id"]


def bronze_table_name(settings: Settings, country_code: str) -> str:
    return settings.table("bronze", f"{country_code.lower()}_register")


@dataclass(frozen=True)
class BronzeResult:
    country_code: str
    table: str
    snapshot_date: date
    rows: int
    corrupt_rows: int
    input_bytes: int
    drift: DriftReport


# --------------------------------------------------------------------------------------
# Header inspection (drift detection input)
# --------------------------------------------------------------------------------------
def actual_csv_header(spark: SparkSession, path: str, contract: SourceContract) -> list[str]:
    reader = spark.read.option("encoding", contract.encoding)
    first = reader.text(path).limit(1).collect()  # spark-lint: ok[SPK001] header line
    if not first:
        raise ValueError(f"Empty landing file: {path}")
    return normalise_header(first[0].value, contract.delimiter)


def actual_json_keys(spark: SparkSession, paths: list[str], sample_rows: int = 20_000) -> list[str]:
    """Union of top-level keys over a sample of NDJSON records (keys are optional per record)."""
    sample = spark.read.text(paths).limit(sample_rows)
    keys = (
        sample.select(F.explode(F.json_object_keys(F.col("value"))).alias("k"))
        .distinct()
        .collect()  # spark-lint: ok[SPK001] distinct key names, a few dozen
    )
    return sorted(r.k for r in keys)


# --------------------------------------------------------------------------------------
# Readers
# --------------------------------------------------------------------------------------
def _all_string_schema(names: list[str], with_corrupt: bool = True) -> StructType:
    fields = [StructField(n, StringType(), True) for n in names]
    if with_corrupt:
        fields.append(StructField(CORRUPT_COL, StringType(), True))
    return StructType(fields)


def read_csv_snapshot(
    spark: SparkSession, snapshot: LandingSnapshot, contract: SourceContract, header: list[str]
) -> DataFrame:
    # Schema built from the *actual* header order, so positions always line up. Positional
    # names (c0, c1, ...) sidestep header quirks such as ' CompanyNumber' or 'RegAddress.PostTown'.
    positional = [f"c{i}" for i in range(len(header))]
    df = (
        spark.read.format("csv")
        .schema(_all_string_schema(positional))
        .option("header", "true")
        .option("sep", contract.delimiter)
        .option("encoding", contract.encoding)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        # multiLine stays off: it would make a 9 GB file a single, unsplittable task.
        .load(snapshot.paths)
    )
    renamed = [F.col(p).alias(bronze_column_name(h)) for p, h in zip(positional, header, strict=True)]
    return df.select(*renamed, F.col(CORRUPT_COL), F.col("_metadata.file_path").alias("_source_file"))


def read_ndjson_snapshot(
    spark: SparkSession, snapshot: LandingSnapshot, contract: SourceContract, keys: list[str]
) -> DataFrame:
    # Declaring nested objects as STRING makes Spark return their raw JSON text: bronze keeps
    # the document as-is, and silver decides how to parse it.
    wanted = [k for k in keys if k not in contract.pii_columns]
    df = (
        spark.read.format("json")
        .schema(_all_string_schema(wanted))
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .load(snapshot.paths)
    )
    return df.select(
        *[F.col(k).alias(bronze_column_name(k)) for k in wanted],
        F.col(CORRUPT_COL),
        F.col("_metadata.file_path").alias("_source_file"),
    )


def conform_to_contract(df: DataFrame, contract: SourceContract) -> DataFrame:
    """Project to the contract's columns. Missing optional ones become NULL, extras go to _rescued_data."""
    expected = list(contract.bronze_columns)
    present = set(df.columns)
    pii = {bronze_column_name(c) for c in contract.pii_columns}
    extras = [c for c in df.columns if c not in expected and c not in METADATA_COLUMNS and c not in pii]
    rescued = F.to_json(F.struct(*[F.col(c) for c in extras])) if extras else F.lit(None).cast("string")
    cols = [F.col(c) if c in present else F.lit(None).cast("string").alias(c) for c in expected]
    return df.select(*cols, rescued.alias("_rescued_data"), F.col(CORRUPT_COL), F.col("_source_file"))


def add_lineage(df: DataFrame, snapshot_date: date, run_id: str) -> DataFrame:
    return df.select(
        "*",
        F.lit(snapshot_date).cast("date").alias("_snapshot_date"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(run_id).alias("_run_id"),
    )


# --------------------------------------------------------------------------------------
# Orchestration for one source
# --------------------------------------------------------------------------------------
def load_bronze(spark: SparkSession, settings: Settings, country_code: str) -> BronzeResult:
    contract = get_contract(country_code)
    files = list_landing_files(spark, settings.landing_root, contract.country)
    snapshot = pick_snapshot(files, contract.country, settings.snapshot_date)
    snapshot_date = snapshot.snapshot_date or settings.snapshot_date

    if snapshot.total_bytes > settings.max_landing_bytes:
        raise RuntimeError(
            f"[{country_code}] landing snapshot is {snapshot.total_bytes:,} bytes, above the guard "
            f"max_landing_bytes={settings.max_landing_bytes:,}. Refusing to run (cost protection)."
        )

    if contract.format == "csv":
        header = actual_csv_header(spark, snapshot.paths[0], contract)
        drift = enforce(detect_drift(country_code, list(contract.columns), header, contract.drift_policy))
        raw = read_csv_snapshot(spark, snapshot, contract, header)
    elif contract.format == "ndjson":
        keys = actual_json_keys(spark, snapshot.paths)
        drift = enforce(
            detect_drift(country_code, list(contract.columns), keys, contract.drift_policy,
                         required=list(contract.required_columns))
        )
        raw = read_ndjson_snapshot(spark, snapshot, contract, sorted(set(keys) | set(contract.columns)))
    else:
        raise ValueError(f"Unsupported format {contract.format}")
    if drift.has_drift:
        log.warning(drift.summary())

    bronze = add_lineage(conform_to_contract(raw, contract), snapshot_date, settings.run_id)
    table = bronze_table_name(settings, country_code)
    ensure_schema(spark, settings.catalog, settings.bronze_schema)
    ddl = ", ".join(f"`{c}` STRING" for c in contract.bronze_columns)
    create_table_if_missing(
        spark, table,
        f"{ddl}, _rescued_data STRING, {CORRUPT_COL} STRING, _source_file STRING, "
        "_snapshot_date DATE NOT NULL, _ingested_at TIMESTAMP, _run_id STRING",
        cluster_by=["_snapshot_date"],
        comment=f"Bronze: raw {contract.register} snapshots, all columns as STRING",
        # Delta collects stats on the first 32 columns only, and clustering keys need stats. Stats on
        # 50 raw string columns are also wasted write time: index only what is filtered on.
        extra_properties={"delta.dataSkippingStatsColumns": "_snapshot_date,_ingested_at"},
    )
    overwrite_partition(bronze, table, f"_snapshot_date = DATE'{snapshot_date.isoformat()}'")

    stats = (
        spark.table(table)
        .where(F.col("_snapshot_date") == F.lit(snapshot_date))
        .agg(F.count(F.lit(1)).alias("rows"), F.count(CORRUPT_COL).alias("corrupt"))
        .first()
    )
    assert stats is not None
    result = BronzeResult(country_code, table, snapshot_date, stats.rows, stats.corrupt,
                          snapshot.total_bytes, drift)
    log.info("bronze %s", json.dumps({**result.__dict__, "drift": drift.summary()}, default=str))
    return result
