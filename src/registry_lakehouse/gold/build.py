"""Silver -> gold: a company dimension and the map-ready aggregates behind the website.

Gold tables are small and fully recomputed from current silver rows, so a run is always
consistent with silver. Each transformation is a pure DataFrame function, so it can be tested
without tables.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from registry_lakehouse.config import Settings
from registry_lakehouse.silver.classify import nace_divisions
from registry_lakehouse.spark import ensure_schema

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
        F.count(F.lit(1)).alias("active_companies"))
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


def build_gold(spark: SparkSession, settings: Settings) -> dict[str, int]:
    ensure_schema(spark, settings.catalog, settings.gold_schema)
    company = spark.table(settings.table("silver", "company")).where("is_current")
    location = spark.table(settings.table("silver", "location"))
    industry = dim_industry(spark)
    dim = dim_company(company, location, industry)
    outputs = {
        "dim_industry": industry,
        "dim_company": dim,
    }
    counts = {}
    for name, df in outputs.items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            settings.table("gold", name))
    # Aggregates read the persisted dimension instead of recomputing its joins three times.
    dim_t = spark.table(settings.table("gold", "dim_company"))
    for name, df in {
        "agg_country_industry": agg_country_industry(dim_t),
        "agg_city_industry": agg_city_industry(dim_t),
        "fishing_by_city": fishing_by_city(dim_t),
    }.items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            settings.table("gold", name))
    for name in ["dim_company", "agg_country_industry", "agg_city_industry", "fishing_by_city"]:
        counts[name] = spark.table(settings.table("gold", name)).count()
    log.info("gold %s", counts)
    return counts
