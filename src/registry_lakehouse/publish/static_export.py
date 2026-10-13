"""Export small, aggregated, privacy-safe JSON for the static website.

Why static: the website never talks to Databricks or AWS. There are no credentials in the
browser, no query endpoint to abuse, no per-viewer cost, and files can be cached by any CDN.
The trade-off is freshness: the data is as fresh as the last pipeline run, which matches how
often the registers publish.

Only aggregates leave the lakehouse (counts per town / industry). Sample rows for the
"pipeline tour" are exported only when ``include_samples`` is set, which the local fixture
run does: those rows are synthetic.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from registry_lakehouse.config import Settings

log = logging.getLogger(__name__)


def _default(o: Any) -> Any:
    if isinstance(o, (date, datetime)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(type(o))


def write_json(root: str, name: str, payload: Any) -> str:
    body = json.dumps(payload, default=_default, ensure_ascii=False, separators=(",", ":"))
    if root.startswith("s3://"):
        import boto3

        bucket, _, prefix = root[5:].partition("/")
        key = f"{prefix.rstrip('/')}/{name}".lstrip("/")
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"),
                                      ContentType="application/json", CacheControl="max-age=3600")
        return f"s3://{bucket}/{key}"
    os.makedirs(root, exist_ok=True)
    path = os.path.join(root, name)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(body)
    return path


def _rows(df: DataFrame, limit: int) -> list[dict]:
    # Gold aggregates are bounded (towns x sections), and the limit makes the bound explicit.
    return [r.asDict(recursive=True) for r in df.limit(limit).collect()]  # spark-lint: ok[SPK001] limited aggregates


def cities_geojson(city_industry: DataFrame, max_cities: int) -> dict:
    per_city = (
        city_industry.where("latitude IS NOT NULL")
        .groupBy("country_code", "city", "latitude", "longitude")
        .agg(F.sum("active_companies").alias("active_companies"),
             F.sum("operating_companies").alias("operating_companies"),
             F.map_from_entries(F.collect_list(F.struct(F.coalesce("nace_section", F.lit("?")),
                                                        "active_companies"))).alias("by_section"))
        .orderBy(F.col("active_companies").desc(), "country_code", "city")
    )
    features = [
        {"type": "Feature",
         "geometry": {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]},
         "properties": {k: r[k] for k in ("country_code", "city", "active_companies", "operating_companies",
                                         "by_section")}}
        for r in _rows(per_city, max_cities)
    ]
    return {"type": "FeatureCollection", "features": features}


def fishing_geojson(fishing: DataFrame) -> dict:
    cols = [c for c in fishing.columns if c not in ("latitude", "longitude")]
    features = [
        {"type": "Feature",
         "geometry": {"type": "Point", "coordinates": [r["longitude"], r["latitude"]]},
         "properties": {c: r[c] for c in cols}}
        for r in _rows(fishing.where("latitude IS NOT NULL").orderBy(F.col("fishing_companies").desc()), 20_000)
    ]
    return {"type": "FeatureCollection", "features": features}


def pipeline_stats(spark: SparkSession, settings: Settings) -> dict:
    """Row counts per stage and country: the funnel shown on the website."""
    from registry_lakehouse.contracts import COUNTRY_CODES

    stages: dict[str, dict[str, int]] = {}

    def per_country(stage: str, df: DataFrame) -> None:
        for r in df.groupBy("country_code").count().collect():  # spark-lint: ok[SPK001] one row per country
            stages.setdefault(stage, {})[r.country_code] = r["count"]

    for cc in COUNTRY_CODES:
        bt = settings.table("bronze", f"{cc.lower()}_register")
        if spark.catalog.tableExists(bt):
            b = spark.table(bt)
            latest = b.agg(F.max("_snapshot_date")).first()[0]
            stages.setdefault("bronze", {})[cc] = b.where(F.col("_snapshot_date") == latest).count()
    per_country("silver_current", spark.table(settings.table("silver", "company")).where("is_current"))
    per_country("quarantined", spark.table(settings.table("silver", "quarantine")))
    dim = spark.table(settings.table("gold", "dim_company"))
    per_country("gold_on_map", dim.where("latitude IS NOT NULL"))
    by_precision = dim.groupBy("country_code", "geo_precision").count()
    precision = {f"{r.country_code}:{r.geo_precision}": r["count"]
                 for r in by_precision.collect()}  # spark-lint: ok[SPK001] countries x 5 precisions
    dq_table = settings.table("ops", "dq_results")
    dq = []
    if spark.catalog.tableExists(dq_table):
        last = spark.table(dq_table).groupBy("country_code").agg(F.max("snapshot_date").alias("snapshot_date"))
        dq = _rows(spark.table(dq_table).join(last, ["country_code", "snapshot_date"])
                   .select("country_code", "snapshot_date", "rule", "severity", "failed_rows", "total_rows",
                           "failure_rate").orderBy("country_code", "rule"), 500)
    return {"stages": stages, "geo_precision": precision, "data_quality": dq}


def stage_samples(spark: SparkSession, settings: Settings, per_country: int = 3) -> dict:
    """The same records followed through bronze -> silver -> gold (synthetic fixtures only)."""
    from registry_lakehouse.contracts import COUNTRY_CODES, get_contract

    out: dict[str, Any] = {}
    company = spark.table(settings.table("silver", "company")).where("is_current")
    dim = spark.table(settings.table("gold", "dim_company"))
    for cc in COUNTRY_CODES:
        bt = settings.table("bronze", f"{cc.lower()}_register")
        if not spark.catalog.tableExists(bt):
            continue
        contract = get_contract(cc)
        # Legal entities only: natural persons are never shown, not even in raw form.
        silver = (company.where((F.col("country_code") == cc) & ~F.coalesce("is_natural_person", F.lit(False)))
                  .orderBy("business_id").limit(per_country))
        keys = [r.business_id for r in silver.select("business_id").collect()]  # spark-lint: ok[SPK001] 3 ids
        source_ids = [k.split("|")[0] for k in keys]  # Navarra keys are "taxid|municipality|street|number"
        bronze = spark.table(bt).where(F.trim(F.col(contract.id_column)).isin(source_ids))
        cols = [contract.id_column, *[c for c in contract.bronze_columns if c != contract.id_column][:13]]
        out[cc] = {
            "bronze": _rows(bronze.select(*cols), per_country * 2),
            "silver": _rows(silver.drop("hash_diff", "_run_id", "_updated_at"), per_country),
            "gold": _rows(dim.where(F.col("business_id").isin(keys)).orderBy("business_id"), per_country),
        }
    return out


def insights(spark: SparkSession, settings: Settings) -> dict:
    """Bounded, aggregate-only extracts of the gold insight tables for the website."""
    def t(name: str) -> DataFrame:
        return spark.table(settings.table("gold", name))

    demography = (t("company_demography").where("year >= 2000")
                  .groupBy("country_code", "year", "survivorship_biased")
                  .agg(F.sum("births").alias("births"), F.sum("deaths").alias("deaths"))
                  .orderBy("country_code", "year"))
    employment = (t("employment_by_region_industry")
                  .groupBy("country_code", "nace_section")
                  .agg(F.sum("employees_low").alias("employees_low"), F.sum("employees_high").alias("employees_high"),
                       F.sum("companies_reporting").alias("companies_reporting"),
                       F.max("has_exact_counts").alias("exact"))
                  .orderBy("country_code", "nace_section"))
    fishing_jobs = (t("employment_by_region_industry").where("nace_division = '03'")
                    .groupBy("country_code", "region")
                    .agg(F.sum("employees_low").alias("employees_low"), F.sum("employees_high").alias("employees_high"),
                         F.sum("companies_reporting").alias("companies"))
                    .where("employees_high > 0")
                    .orderBy(F.col("employees_high").desc()))
    return {
        "activity": _rows(t("company_activity_summary").orderBy("country_code"), 10),
        "hotspots": _rows(t("registered_office_hotspots").orderBy(F.col("companies").desc())
                          .select("country_code", "city", "postal_code", "address_line", "companies",
                                  "dormant_companies", "pending_strike_off", "distinct_industries"), 25),
        "demography": _rows(demography, 1000),
        "survival": _rows(t("cohort_survival").where("nace_section = 'ALL' AND cohort_year >= 1995")
                          .orderBy("country_code", "cohort_year"), 200),
        "survival_by_section": _rows(t("cohort_survival").where("nace_section <> 'ALL'")
                                     .orderBy("country_code", "nace_section"), 100),
        "fishing_hubs": _rows(t("fishing_hubs").orderBy(F.col("location_quotient").desc(), "city"), 50),
        "employment": _rows(employment, 200),
        "fishing_employment": _rows(fishing_jobs, 30),
        "distress": _rows(t("distress_by_industry").orderBy("country_code", "nace_section"), 200),
        "coverage": _rows(t("insight_coverage").orderBy("insight", "country_code"), 100),
    }


def publish(spark: SparkSession, settings: Settings, *, include_samples: bool = False,
            max_cities: int = 5000) -> list[str]:
    gold = settings.publish_root
    written = [
        write_json(gold, "cities.geojson", cities_geojson(spark.table(settings.table("gold", "agg_city_industry")),
                                                          max_cities)),
        write_json(gold, "fishing.geojson", fishing_geojson(spark.table(settings.table("gold", "fishing_by_city")))),
        write_json(gold, "industry_by_country.json",
                   _rows(spark.table(settings.table("gold", "agg_country_industry"))
                         .orderBy("country_code", "nace_section"), 10_000)),
        write_json(gold, "pipeline.json", pipeline_stats(spark, settings)),
        write_json(gold, "insights.json", insights(spark, settings)),
    ]
    if include_samples:
        written.append(write_json(gold, "stage_samples.json", stage_samples(spark, settings)))
    written.append(write_json(gold, "meta.json", {"generated_at": datetime.utcnow().isoformat() + "Z",
                                                  "run_id": settings.run_id,
                                                  "attribution": "Postcode centroids: GeoNames (CC BY 4.0)"}))
    log.info("published %s", written)
    return written
