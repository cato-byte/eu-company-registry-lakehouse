"""The whole pipeline, locally, on the synthetic fixtures: landing -> bronze -> silver -> geo -> gold -> publish.

This is the test that replaces "run it on Databricks and see". It takes a few minutes on a
laptop and costs nothing.
"""

import json
import shutil
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from registry_lakehouse import cli
from registry_lakehouse.contracts import COUNTRY_CODES
from registry_lakehouse.geo.geocode import COUNTRY_BBOX
from tests.conftest import LANDING

pytestmark = [pytest.mark.spark, pytest.mark.e2e]


@pytest.fixture(scope="module")
def run(spark, tmp_path_factory):
    from registry_lakehouse.config import Settings

    publish = tmp_path_factory.mktemp("publish")
    # Work on a copy: the run writes geocoding request files under <landing>/_GEOCODE.
    landing = tmp_path_factory.mktemp("e2e") / "landing"
    shutil.copytree(LANDING, landing)
    s = Settings(landing_root=str(landing), bronze_schema="e2e_bronze", silver_schema="e2e_silver",
                 gold_schema="e2e_gold", ops_schema="e2e_ops", publish_root=str(publish),
                 snapshot_date=date(2026, 7, 31), run_id="e2e-1")
    countries = list(COUNTRY_CODES)
    cli.run_bronze(spark, s, countries)
    cli.run_silver(spark, s, countries)
    cli.run_reference(spark, s)
    cli.run_geo(spark, s, batch=50)
    cli.run_gold(spark, s)
    cli.run_publish(spark, s, samples=True)
    return s


def test_every_country_reaches_gold(spark, run):
    current = spark.table(run.table("silver", "company")).where("is_current")
    counts = {r.country_code: r["count"] for r in current.groupBy("country_code").count().collect()}
    assert set(counts) == set(COUNTRY_CODES)
    assert spark.table(run.table("gold", "dim_company")).count() == current.count()


def test_quarantine_holds_the_invalid_siret(spark, run):
    q = spark.table(run.table("silver", "quarantine")).collect()
    assert [(r.country_code, r.failures) for r in q] == [("FR", ["id_checksum_valid"])]


def test_fishing_companies_found_in_every_country_with_industry_codes(spark, run):
    fish = spark.table(run.table("gold", "fishing_by_city"))
    by_country = {r.country_code: r.n for r in fish.groupBy("country_code").agg(F.sum("fishing_companies").alias("n")).collect()}
    # Estonia's basic-data file carries no activity code, so it cannot appear here.
    assert {"FR", "GB", "IE", "NO", "ES"} <= set(by_country)
    assert all(n > 0 for n in by_country.values())


def test_most_companies_are_on_the_map(spark, run):
    dim = spark.table(run.table("gold", "dim_company")).where("is_active")
    located = dim.where("latitude IS NOT NULL").count()
    assert located / dim.count() > 0.8
    # every coordinate is inside its register's coverage area
    for r in dim.where("latitude IS NOT NULL").select("country_code", "latitude", "longitude").collect():
        x0, y0, x1, y1 = COUNTRY_BBOX[r.country_code]
        assert x0 <= r.longitude <= x1 and y0 <= r.latitude <= y1, r


def test_geocoding_requests_exported_for_the_gradual_worker(run):
    req_dir = Path(run.landing_root) / "_GEOCODE" / "requests" / f"run_id={run.run_id}"
    lines = [json.loads(line) for f in req_dir.glob("*.json") for line in f.read_text().splitlines() if line]
    assert 0 < len(lines) <= 50
    assert all(line["address_line"] for line in lines)


def test_published_files_are_small_and_contain_no_personal_names(run):
    out = Path(run.publish_root)
    names = {p.name for p in out.iterdir()}
    assert {"cities.geojson", "fishing.geojson", "industry_by_country.json", "pipeline.json", "meta.json",
            "stage_samples.json"} <= names
    cities = json.loads((out / "cities.geojson").read_text())
    assert cities["features"] and all(f["geometry"]["type"] == "Point" for f in cities["features"])
    total = sum(p.stat().st_size for p in out.iterdir())
    assert total < 2_000_000
    # Natural-person names from the fixtures (FIE / ENK / Spanish DNI rows) must not leak anywhere.
    published = "\n".join(p.read_text(encoding="utf-8") for p in out.iterdir())
    ee = (LANDING / "ESTONIA").rglob("*.csv")
    persons = [line.split(";")[0] for f in ee for line in f.read_text(encoding="utf-8-sig").splitlines()
               if "Füüsilisest isikust" in line]
    assert persons and not [p for p in persons if p in published]


def test_second_snapshot_builds_scd2_history(spark, run):
    later = replace(run, snapshot_date=date(2026, 8, 31), run_id="e2e-2")
    cli.run_bronze(spark, later, ["GB"])
    cli.run_silver(spark, later, ["GB"])
    gb = spark.table(run.table("silver", "company")).where("country_code = 'GB'")
    snap2 = date(2026, 8, 1)
    assert gb.where(F.col("valid_to") == snap2).count() == 3  # liquidation + move + disappeared
    assert gb.where((F.col("valid_from") == snap2) & F.col("is_current")).count() == 3  # 2 new versions + 1 new co
    assert gb.where("is_current AND status = 'LIQUIDATION'").count() >= 1
    # idempotent: re-running the same snapshot changes nothing
    before = gb.count()
    cli.run_silver(spark, later, ["GB"])
    assert spark.table(run.table("silver", "company")).where("country_code = 'GB'").count() == before


def test_delta_history_and_change_data_feed(spark, run):
    table = run.table("silver", "company")
    history = spark.sql(f"DESCRIBE HISTORY {table}").select("operation").collect()
    assert "MERGE" in {h.operation for h in history}
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    assert props.get("delta.enableChangeDataFeed") == "true"
    changes = spark.read.option("readChangeFeed", "true").option("startingVersion", 0).table(table)
    assert changes.where("_change_type = 'update_postimage'").count() > 0
