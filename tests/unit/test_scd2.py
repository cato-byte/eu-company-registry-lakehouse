"""SCD Type 2 merge of full snapshots, built from hand-made canonical rows."""

from datetime import date

import pytest
from pyspark.sql import functions as F

from registry_lakehouse.silver.canonical import CANONICAL_ORDER, MAPPED, MAPPED_COLUMNS, TRACKED, hash_diff
from registry_lakehouse.silver.scd2 import SnapshotOrderError, apply_snapshot, ensure_company_table

pytestmark = pytest.mark.spark
TABLE = "scd_test.company"


def snapshot(spark, rows, snap, country="GB"):
    """rows: list of dicts with business_id + any tracked overrides."""
    full = []
    for r in rows:
        base = {c: None for c in MAPPED}
        base.update({"business_id": r["business_id"], "status": "ACTIVE", "company_name": "X"})
        base.update(r)
        full.append(base)
    df = spark.createDataFrame(full, ", ".join(f"{name} {sql_type}" for name, sql_type, _ in MAPPED_COLUMNS))
    return df.select(
        F.sha2(F.concat_ws("|", F.lit(country), "business_id"), 256).alias("company_key"),
        F.lit("loc").alias("location_key"), F.lit(country).alias("country_code"), F.lit("TEST").alias("source_register"),
        F.lit("LEGAL_ENTITY").alias("grain"), *MAPPED, F.lit(None).cast("string").alias("nace_division"),
        F.lit(None).cast("string").alias("nace_section"), (F.col("status") == "ACTIVE").alias("is_active"),
        hash_diff(TRACKED).alias("hash_diff"), F.lit(snap).cast("date").alias("snapshot_date"),
    ).select(*CANONICAL_ORDER)


@pytest.fixture
def table(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS scd_test")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    ensure_company_table(spark, TABLE)
    return TABLE


def history(spark, business_id):
    return [(r.status, r.city, r.valid_from, r.valid_to, r.is_current) for r in
            spark.table(TABLE).where(F.col("business_id") == business_id).orderBy("valid_from", "valid_to").collect()]


def test_full_lifecycle(spark, table):
    d1, d2, d3 = date(2026, 6, 1), date(2026, 7, 1), date(2026, 8, 1)
    apply_snapshot(spark, table, snapshot(spark, [{"business_id": "A", "city": "LEEDS"},
                                                  {"business_id": "B"}, {"business_id": "C"}], d1), "GB", d1, "r1")
    # A moves, B unchanged, C disappears, D is new
    m = apply_snapshot(spark, table, snapshot(spark, [{"business_id": "A", "city": "YORK"}, {"business_id": "B"},
                                                      {"business_id": "D"}], d2), "GB", d2, "r2")
    assert m == {"inserted": 2, "closed": 2, "skipped": 0}
    assert history(spark, "A") == [("ACTIVE", "LEEDS", d1, d2, False), ("ACTIVE", "YORK", d2, None, True)]
    assert history(spark, "B") == [("ACTIVE", None, d1, None, True)]
    assert history(spark, "C") == [("ACTIVE", None, d1, d2, False)]
    assert history(spark, "D") == [("ACTIVE", None, d2, None, True)]

    # A goes into liquidation
    apply_snapshot(spark, table, snapshot(spark, [{"business_id": "A", "city": "YORK", "status": "LIQUIDATION"},
                                                  {"business_id": "B"}, {"business_id": "D"}], d3), "GB", d3, "r3")
    assert [h[0] for h in history(spark, "A")] == ["ACTIVE", "ACTIVE", "LIQUIDATION"]
    current = spark.table(table).where("is_current")
    assert current.count() == current.select("company_key").distinct().count() == 3


def test_rerun_of_same_snapshot_is_a_no_op(spark, table):
    d1 = date(2026, 6, 1)
    snap = snapshot(spark, [{"business_id": "A"}], d1)
    apply_snapshot(spark, table, snap, "GB", d1, "r1")
    version = spark.sql(f"DESCRIBE HISTORY {table}").agg(F.max("version")).first()[0]
    assert apply_snapshot(spark, table, snap, "GB", d1, "r1-again")["skipped"] == 1
    assert spark.sql(f"DESCRIBE HISTORY {table}").agg(F.max("version")).first()[0] == version


def test_out_of_order_snapshot_is_rejected(spark, table):
    apply_snapshot(spark, table, snapshot(spark, [{"business_id": "A"}], date(2026, 7, 1)), "GB", date(2026, 7, 1), "r")
    with pytest.raises(SnapshotOrderError, match="RESTORE"):
        apply_snapshot(spark, table, snapshot(spark, [{"business_id": "A"}], date(2026, 6, 1)), "GB",
                       date(2026, 6, 1), "r")


def test_other_countries_are_never_touched(spark, table):
    d1, d2 = date(2026, 6, 1), date(2026, 7, 1)
    apply_snapshot(spark, table, snapshot(spark, [{"business_id": "A"}], d1, "IE"), "IE", d1, "r")
    apply_snapshot(spark, table, snapshot(spark, [{"business_id": "Z"}], d2, "GB"), "GB", d2, "r")
    assert spark.table(table).where("country_code = 'IE' AND is_current").count() == 1


def test_constraints_reject_invalid_rows(spark, table):
    bad = snapshot(spark, [{"business_id": "A", "status": "WEIRD"}], date(2026, 6, 1))
    with pytest.raises(Exception, match=r"(?i)constraint|CHECK"):
        apply_snapshot(spark, table, bad, "GB", date(2026, 6, 1), "r")


def test_time_travel_gives_reproducible_history(spark, table):
    d1, d2 = date(2026, 6, 1), date(2026, 7, 1)
    apply_snapshot(spark, table, snapshot(spark, [{"business_id": "A", "city": "LEEDS"}], d1), "GB", d1, "r")
    v1 = spark.sql(f"DESCRIBE HISTORY {table}").agg(F.max("version")).first()[0]
    apply_snapshot(spark, table, snapshot(spark, [{"business_id": "A", "city": "YORK"}], d2), "GB", d2, "r")
    old = spark.read.option("versionAsOf", v1).table(table).where("is_current").first()
    assert old.city == "LEEDS"
