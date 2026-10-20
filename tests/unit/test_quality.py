"""Data-quality engine: one-pass annotation, quarantine split, thresholds, volume anomalies."""

from datetime import date

import pytest
from pyspark.sql import functions as F

from registry_lakehouse.quality import engine
from registry_lakehouse.quality.rules import RULES, Rule, rules_for

pytestmark = pytest.mark.spark

RULE_SET = [
    Rule("id_present", lambda: F.col("id").isNotNull(), "ERROR", "id must exist", max_failure_rate=0.10),
    Rule("name_present", lambda: F.col("name").isNotNull(), "WARN", "name should exist"),
]


@pytest.fixture
def df(spark):
    rows = [("1", "a"), ("2", None), (None, "c"), ("4", "d")]
    return spark.createDataFrame(rows, "id string, name string")


def evaluate(df, total_rows_min=1, previous=None):
    annotated = engine.annotate(df, RULE_SET)
    return annotated, engine.evaluate(annotated, RULE_SET, dataset="t", country="XX", snapshot_date=date(2026, 7, 1),
                                      run_id="r", previous_total=previous, min_rows_for_rate=total_rows_min)


def test_annotate_flags_and_quarantines_only_error_rules(df):
    annotated, _ = evaluate(df)
    rows = {(r.id, r.name): (r[engine.FAILURES_COL], r[engine.QUARANTINE_COL]) for r in annotated.collect()}
    assert rows[("2", None)] == (["name_present"], False)  # WARN: kept
    assert rows[(None, "c")] == (["id_present"], True)  # ERROR: quarantined
    assert rows[("1", "a")] == ([], False)


def test_threshold_breach_is_reported(df):
    _, outcome = evaluate(df)
    assert not outcome.ok
    assert outcome.breaches[0].startswith("id_present: 25.00% > 10.00%")
    metric = next(m for m in outcome.metrics if m["rule"] == "id_present")
    assert metric["failed_rows"] == 1 and metric["total_rows"] == 4 and metric["breached"] == "true"


def test_thresholds_need_a_minimum_sample(df):
    _, outcome = evaluate(df, total_rows_min=1000)
    assert outcome.ok


def test_volume_drop_against_previous_snapshot_is_fatal(df):
    _, outcome = evaluate(df.where("id = '1' OR id IS NULL OR id = '2'"), total_rows_min=10_000, previous=10)
    assert any(b.startswith("row_volume") for b in outcome.breaches)


def test_malformed_source_lines_breach_their_own_threshold(df):
    annotated = engine.annotate(df, RULE_SET)
    outcome = engine.evaluate(annotated, RULE_SET[1:], dataset="t", country="XX", snapshot_date=date(2026, 7, 1),
                              run_id="r", min_rows_for_rate=1, malformed_rows=1, max_malformed_rate=0.10)
    assert outcome.breaches == ["malformed_source_line: 20.00% > 10.00% (1/5)"]
    metric = next(m for m in outcome.metrics if m["rule"] == "malformed_source_line")
    assert metric["failed_rows"] == 1 and metric["total_rows"] == 5


def test_empty_snapshot_is_fatal(spark):
    empty = spark.createDataFrame([], "id string, name string")
    _, outcome = evaluate(empty)
    assert "empty snapshot" in outcome.breaches[-1]


def test_rules_are_well_formed():
    names = [r.name for r in RULES]
    assert len(names) == len(set(names))
    assert {r.severity for r in RULES} <= {"WARN", "ERROR"}
    assert all(r.description for r in RULES)
    assert "name_present_for_legal_entities" not in {r.name for r in rules_for("FR")}  # FR names often absent


def test_metrics_are_persisted(spark, df):
    _, outcome = evaluate(df)
    spark.sql("CREATE SCHEMA IF NOT EXISTS dq_test")
    engine.write_metrics(spark, "dq_test.dq_results", outcome)
    assert spark.table("dq_test.dq_results").count() == len(outcome.metrics)
    assert engine.previous_row_count(spark, "dq_test.dq_results", "t", "XX", date(2026, 8, 1)) == 4
