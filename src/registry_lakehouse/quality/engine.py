"""Evaluate rules in a single pass, split valid/quarantine, and persist metrics.

All rules are evaluated in one projection, and all metrics come from a single aggregation:
one scan, however many rules there are.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from registry_lakehouse.quality.rules import Rule

log = logging.getLogger(__name__)

FAILURES_COL = "_dq_failures"
QUARANTINE_COL = "_dq_quarantine"

DQ_RESULTS_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("dataset", StringType(), False),
    StructField("country_code", StringType(), False),
    StructField("snapshot_date", DateType(), False),
    StructField("rule", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("failed_rows", LongType(), False),
    StructField("total_rows", LongType(), False),
    StructField("failure_rate", DoubleType(), False),
    StructField("threshold", DoubleType(), True),
    StructField("breached", StringType(), False),
    StructField("evaluated_at", TimestampType(), False),
])


class DataQualityError(RuntimeError):
    pass


@dataclass
class DqOutcome:
    total_rows: int
    metrics: list[dict]
    breaches: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.breaches


def annotate(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Add the array of failed rule names and the quarantine flag."""
    failed = F.array_compact(F.array(*[F.when(~F.coalesce(r.check(), F.lit(False)), F.lit(r.name)) for r in rules]))
    error_names = [r.name for r in rules if r.severity == "ERROR"]
    quarantine = F.exists(failed, lambda n: n.isin(error_names)) if error_names else F.lit(False)
    return df.withColumns({FAILURES_COL: failed, QUARANTINE_COL: quarantine})


def measure(annotated: DataFrame, rules: list[Rule]) -> tuple[int, dict[str, int]]:
    aggs = [F.count(F.lit(1)).alias("__total")] + [
        F.sum(F.array_contains(F.col(FAILURES_COL), r.name).cast("long")).alias(r.name) for r in rules
    ]
    row = annotated.agg(*aggs).first()
    assert row is not None
    total = int(row["__total"])
    return total, {r.name: int(row[r.name] or 0) for r in rules}


def evaluate(
    annotated: DataFrame,
    rules: list[Rule],
    *,
    dataset: str,
    country: str,
    snapshot_date: date,
    run_id: str,
    previous_total: int | None = None,
    min_volume_ratio: float = 0.8,
    min_rows_for_rate: int = 1000,
) -> DqOutcome:
    """Rate thresholds only apply from ``min_rows_for_rate`` rows on: 1 bad id in 37 rows is
    noise, while 1% of 40 million rows is a structural problem."""
    total, failed = measure(annotated, rules)
    metrics, breaches = [], []
    for r in rules:
        rate = failed[r.name] / total if total else 0.0
        breached = r.max_failure_rate is not None and total >= min_rows_for_rate and rate > r.max_failure_rate
        if breached:
            breaches.append(f"{r.name}: {rate:.2%} > {r.max_failure_rate:.2%} ({failed[r.name]:,}/{total:,})")
        metrics.append(dict(run_id=run_id, dataset=dataset, country_code=country, snapshot_date=snapshot_date,
                            rule=r.name, severity=r.severity, failed_rows=failed[r.name], total_rows=total,
                            failure_rate=rate, threshold=r.max_failure_rate, breached=str(breached).lower()))
    # Volume anomaly: a register does not lose 20% of its companies overnight. A truncated
    # download does.
    if previous_total:
        ratio = total / previous_total
        breached = ratio < min_volume_ratio
        if breached:
            breaches.append(f"row_volume: {total:,} rows is {ratio:.0%} of previous snapshot ({previous_total:,})")
        metrics.append(dict(run_id=run_id, dataset=dataset, country_code=country, snapshot_date=snapshot_date,
                            rule="row_volume_vs_previous", severity="FATAL", failed_rows=max(previous_total - total, 0),
                            total_rows=total, failure_rate=max(0.0, 1 - ratio), threshold=1 - min_volume_ratio,
                            breached=str(breached).lower()))
    if total == 0:
        breaches.append("empty snapshot: 0 rows after mapping")
    return DqOutcome(total, metrics, breaches)


def write_metrics(spark: SparkSession, table: str, outcome: DqOutcome) -> None:
    if not outcome.metrics:
        return
    rows = [{**m, "evaluated_at": None} for m in outcome.metrics]
    nullable = StructType([StructField(f.name, f.dataType, True) for f in DQ_RESULTS_SCHEMA.fields])
    df = spark.createDataFrame(rows, nullable).withColumn("evaluated_at", F.current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(table)


def previous_row_count(spark: SparkSession, table: str, dataset: str, country: str, before: date) -> int | None:
    if not spark.catalog.tableExists(table):
        return None
    row = (
        spark.table(table)
        .where((F.col("dataset") == dataset) & (F.col("country_code") == country)
               & (F.col("snapshot_date") < F.lit(before)))
        .orderBy(F.col("snapshot_date").desc(), F.col("evaluated_at").desc())
        .select("total_rows")
        .first()
    )
    return int(row.total_rows) if row else None
