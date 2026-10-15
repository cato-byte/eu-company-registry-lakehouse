"""Change detection: fingerprints, data versions that ignore maintenance, state, adoption."""

from collections import namedtuple
from datetime import date

import pytest

from registry_lakehouse.config import Settings
from registry_lakehouse.ops import freshness

pytestmark = pytest.mark.spark
File = namedtuple("File", "path size_bytes")


@pytest.fixture
def settings():
    return Settings(landing_root="/nowhere", ops_schema="fresh_ops", silver_schema="fresh_silver",
                    snapshot_date=date(2026, 10, 6), run_id="t")


def test_files_fingerprint_is_order_independent_and_size_sensitive():
    a, b = File("s3://x/FR/a.csv", 10), File("s3://x/FR/b.csv", 20)
    assert freshness.files_fingerprint([a, b]) == freshness.files_fingerprint([b, a])
    assert freshness.files_fingerprint([a, b]) != freshness.files_fingerprint([a, File("s3://x/FR/b.csv", 21)])
    assert freshness.files_fingerprint([]) == freshness.files_fingerprint([])


def test_data_version_ignores_maintenance_commits(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS fresh_silver")
    spark.sql("DROP TABLE IF EXISTS fresh_silver.t")
    spark.range(5).write.format("delta").saveAsTable("fresh_silver.t")
    v = freshness.data_version(spark, "fresh_silver.t")
    spark.sql("OPTIMIZE fresh_silver.t")
    spark.sql("ALTER TABLE fresh_silver.t SET TBLPROPERTIES ('comment.owner' = 'x')")
    assert freshness.data_version(spark, "fresh_silver.t") == v  # nothing to recompute
    spark.range(3).write.format("delta").mode("append").saveAsTable("fresh_silver.t")
    assert freshness.data_version(spark, "fresh_silver.t") > v
    assert freshness.data_version(spark, "fresh_silver.missing") == -1


def test_state_roundtrip(spark, settings):
    assert freshness.last_fingerprint(spark, settings, "gold") is None
    freshness.record(spark, settings, "gold", "abc")
    freshness.record(spark, settings, "gold", "def")  # upsert, one row per stage
    assert freshness.last_fingerprint(spark, settings, "gold") == "def"
    assert freshness.unchanged(spark, settings, "gold", "def")
    assert not freshness.unchanged(spark, settings, "gold", "xyz")
    assert spark.table(settings.table("ops", "stage_state")).where("stage = 'gold'").count() == 1


def test_adoption_only_when_outputs_are_newer_than_inputs(spark, settings):
    spark.sql("CREATE SCHEMA IF NOT EXISTS fresh_silver")
    for t in ("inp", "out"):
        spark.sql(f"DROP TABLE IF EXISTS fresh_silver.{t}")
    spark.range(1).write.format("delta").saveAsTable("fresh_silver.inp")
    spark.range(1).write.format("delta").saveAsTable("fresh_silver.out")
    kw = {"outputs": ["fresh_silver.out"], "inputs": ["fresh_silver.inp"]}
    assert freshness.unchanged(spark, settings, "stage_a", "fp1", **kw)  # adopted, fingerprint recorded
    assert freshness.last_fingerprint(spark, settings, "stage_a") == "fp1"
    # input changes after the output: no adoption for a stage without history
    spark.range(1).write.format("delta").mode("append").saveAsTable("fresh_silver.inp")
    assert not freshness.unchanged(spark, settings, "stage_b", "fp1", **kw)
