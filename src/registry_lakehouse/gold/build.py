"""Silver -> gold: a company dimension and the map-ready aggregates behind the website.

Gold tables are small and fully recomputed from current silver rows, so a run is always
consistent with silver. Each transformation is a pure DataFrame function, so it can be tested
without tables.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from registry_lakehouse.config import Settings
from registry_lakehouse.gold import insights as ins
from registry_lakehouse.ops import freshness
from registry_lakehouse.ops.alerts import notify
from registry_lakehouse.silver.classify import nace_divisions
from registry_lakehouse.spark import ensure_schema, table_exists

log = logging.getLogger(__name__)

FISHING_CLASSES = {
    "03.11": "Marine fishing",
    "03.12": "Freshwater fishing",
    "03.21": "Marine aquaculture",
    "03.22": "Freshwater aquaculture",
}


def dim_industry(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(nace_divisions()).select(
        F.col("division").alias("nace_division"), F.col("section").alias("nace_section"),
        "section_label", "division_label")


def dim_company(company_current: DataFrame, location: DataFrame, industry: DataFrame) -> DataFrame:
    geo = location.select("location_key", "latitude", "longitude", "geo_precision", "geo_source")
    return (
        company_current.join(geo, "location_key", "left")
        .join(F.broadcast(industry.select("nace_division", "division_label")), "nace_division", "left")
        .join(F.broadcast(industry.select("nace_section", "section_label").distinct()), "nace_section", "left")
        .select(
            "company_key", "country_code", "grain", "source_register", "business_id",
            # Natural persons never carry a name in silver. This guards against a future mapper bug.
            F.when(~F.coalesce("is_natural_person", F.lit(False)), F.col("company_name")).alias("company_name"),
            "is_natural_person", "legal_form_category", "status", "is_active", "registration_date",
            "nace_code", "nace_division", "division_label", "nace_section", "section_label",
            "employees_min", "employees_max", "postal_code", "city", "region",
            "location_key", F.col("address_line").isNotNull().alias("has_street_address"), "address_line",
            "closure_date", "is_dormant", "pending_strike_off",
            "latitude", "longitude", F.coalesce("geo_precision", F.lit("NONE")).alias("geo_precision"),
            "geo_source", F.col("valid_from").alias("version_valid_from"),
        )
    )


def agg_country_industry(dim: DataFrame) -> DataFrame:
    return dim.groupBy("country_code", "nace_section", "section_label").agg(
        F.count(F.lit(1)).alias("companies"),
        F.sum(F.col("is_active").cast("int")).alias("active_companies"),
        F.sum("employees_min").alias("employees_lower_bound"),
    )


def agg_city_industry(dim: DataFrame) -> DataFrame:
    """Active companies per town and NACE section, with a town centroid for the map."""
    active = dim.where("is_active AND city IS NOT NULL")
    centroid = active.where("latitude IS NOT NULL").groupBy("country_code", "city").agg(
        F.round(F.avg("latitude"), 5).alias("latitude"), F.round(F.avg("longitude"), 5).alias("longitude"))
    counts = active.groupBy("country_code", "city", "nace_section", "section_label").agg(
        F.count(F.lit(1)).alias("active_companies"),
        F.sum(F.col("is_operating").cast("int")).alias("operating_companies"))
    return counts.join(centroid, ["country_code", "city"], "left")


def fishing_by_city(dim: DataFrame) -> DataFrame:
    """Active fishing and aquaculture companies (NACE division 03) per town, split by class."""
    fish = dim.where("is_active AND nace_division = '03'")
    return fish.groupBy("country_code", "city").agg(
        F.count(F.lit(1)).alias("fishing_companies"),
        F.round(F.avg("latitude"), 5).alias("latitude"),
        F.round(F.avg("longitude"), 5).alias("longitude"),
        *[F.sum(F.when(F.col("nace_code") == code, 1).otherwise(0)).alias(label.lower().replace(" ", "_"))
          for code, label in FISHING_CLASSES.items()],
    )


def schema_changes(old: StructType, new: StructType) -> list[str]:
    """Column-level differences (added, removed, retyped). Nullability is ignored."""
    before = {f.name: f.dataType.simpleString() for f in old.fields}
    after = {f.name: f.dataType.simpleString() for f in new.fields}
    return ([f"+{c} {t}" for c, t in after.items() if c not in before]
            + [f"-{c}" for c in before if c not in after]
            + [f"~{c} {before[c]} -> {t}" for c, t in after.items() if c in before and before[c] != t])


def _save(spark: SparkSession, settings: Settings, name: str, df: DataFrame) -> None:
    """Full overwrite. Gold schemas follow the code, but a change is announced, never silent:
    the website export and any SQL consumer read these columns."""
    table = settings.table("gold", name)
    if table_exists(spark, table):
        changes = schema_changes(spark.table(table).schema, df.schema)
        if changes:
            notify("WARN", f"schema change in {table}", ", ".join(changes))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def gold_fingerprint(spark: SparkSession, settings: Settings) -> str:
    return freshness.fingerprint({
        "tables": freshness.data_versions(
            spark, [settings.table("silver", t) for t in ("company", "location", "ref_place")]),
        "settings": [settings.mass_address_min_companies, settings.fishing_hub_min_companies],
    })


def build_gold(spark: SparkSession, settings: Settings) -> dict[str, int] | None:
    """Rebuild all gold tables, unless silver and the insight settings are unchanged (None)."""
    ensure_schema(spark, settings.catalog, settings.gold_schema)
    fp = gold_fingerprint(spark, settings)
    if freshness.unchanged(spark, settings, "gold", fp,
                           outputs=[settings.table("gold", t) for t in ("dim_company", "fishing_hubs")],
                           inputs=[settings.table("silver", t) for t in ("company", "location", "ref_place")]):
        return None
    silver = spark.table(settings.table("silver", "company"))
    company = silver.where("is_current")
    location = spark.table(settings.table("silver", "location"))
    industry = dim_industry(spark)
    _save(spark, settings, "dim_industry", industry)

    # Mass-registration addresses are found on the plain dimension, then flagged on it.
    base = dim_company(company, location, industry)
    _save(spark, settings, "registered_office_hotspots",
          ins.mass_addresses(base, settings.mass_address_min_companies))
    hotspots = spark.table(settings.table("gold", "registered_office_hotspots"))
    _save(spark, settings, "dim_company", ins.flag_operating(base, hotspots))

    # Everything else reads the persisted dimension instead of recomputing its joins.
    dim_t = spark.table(settings.table("gold", "dim_company"))
    as_of_row = silver.agg(F.max("snapshot_date")).first()
    as_of = as_of_row[0] if as_of_row and as_of_row[0] else settings.snapshot_date
    towns_table = settings.table("silver", "ref_place")
    towns = spark.table(towns_table) if table_exists(spark, towns_table) else None
    outputs = {
        "agg_country_industry": agg_country_industry(dim_t),
        "agg_city_industry": agg_city_industry(dim_t),
        "fishing_by_city": fishing_by_city(dim_t),
        "company_activity_summary": ins.company_activity_summary(dim_t),
        "company_demography": ins.company_demography(silver, as_of),
        "cohort_survival": ins.cohort_survival(company, as_of),
        "fishing_hubs": ins.fishing_hubs(dim_t, towns, settings.fishing_hub_min_companies),
        "employment_by_region_industry": ins.employment_by_region_industry(dim_t),
        "distress_by_industry": ins.distress_by_industry(dim_t),
        "insight_coverage": ins.insight_coverage(spark),
    }
    for name, df in outputs.items():
        _save(spark, settings, name, df)
    counts = {name: spark.table(settings.table("gold", name)).count()
              for name in ["dim_company", "registered_office_hotspots", *outputs]}
    log.info("gold %s", counts)
    freshness.record(spark, settings, "gold", fp)
    return counts
