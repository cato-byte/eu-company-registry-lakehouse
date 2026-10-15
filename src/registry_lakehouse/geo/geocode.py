"""Tiered, free geocoding of every distinct company location.

Most precise tier first. Every candidate must fall inside its country's bounding box:

  1. ADDRESS   external results cached from the gradual geocoding worker (Lambda)
  2. ADDRESS   coordinates published by the register (France, Lambert-93)
  3. POSTCODE  GeoNames postcode centroid (full postcode for the UK)
  4. DISTRICT  UK outward code / Irish Eircode routing key centroid
  5. CITY      GeoNames place or municipality centroid
  6. NONE      left off the map, and counted

Tiers 2-5 are plain joins against an offline gazetteer, so a full run makes no external calls.
Tier 1 is filled gradually: each run exports a bounded batch of the most valuable coarse
locations for the worker, so the map's precision improves run after run at zero cost.
"""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from registry_lakehouse.config import Settings
from registry_lakehouse.geo.lambert93 import lambert93_to_wgs84
from registry_lakehouse.geo.reference import (
    gb_district_centroids,
    norm_place,
    place_centroids,
    places_table,
    postcode_centroids,
    reference_table,
)
from registry_lakehouse.spark import create_table_if_missing, ensure_schema, table_exists

log = logging.getLogger(__name__)

# Coverage area of each register: generous mainland + islands boxes (lon_min, lat_min, lon_max, lat_max).
COUNTRY_BBOX = {
    "FR": (-5.5, 41.2, 9.8, 51.2),
    "GB": (-8.7, 49.8, 1.9, 60.95),
    "IE": (-10.7, 51.3, -5.9, 55.5),
    "EE": (21.7, 57.4, 28.3, 59.8),
    # The Spanish source is Navarra's regional register: anything outside Navarra is a wrong match
    # (e.g. a town name that also exists near Madrid).
    "ES": (-2.6, 41.85, -0.6, 43.35),
    "NO": (4.0, 57.9, 31.3, 71.3),
}
PRECISION_RANK = {"ADDRESS": 1, "POSTCODE": 2, "DISTRICT": 3, "CITY": 4, "NONE": 9}


def _in_country(country: Column, lon: Column, lat: Column) -> Column:
    cond = F.lit(False)
    for cc, (x0, y0, x1, y1) in COUNTRY_BBOX.items():
        cond = cond | ((country == cc) & lon.between(x0, x1) & lat.between(y0, y1))
    return cond


def distinct_locations(company_current: DataFrame) -> DataFrame:
    """One row per location_key among current companies (natural persons have no street, so
    their location is postcode/city only, by construction)."""
    return (
        company_current.where(F.col("address_country").isNull() | (F.col("address_country") == F.col("country_code")))
        .groupBy("location_key")
        .agg(
            F.first("country_code").alias("country_code"),
            F.first("address_line", ignorenulls=True).alias("address_line"),
            F.first("postal_code", ignorenulls=True).alias("postal_code"),
            F.first("city", ignorenulls=True).alias("city"),
            F.first("municipality_code", ignorenulls=True).alias("municipality_code"),
            F.avg("source_x").alias("source_x"),
            F.avg("source_y").alias("source_y"),
            F.first("source_crs", ignorenulls=True).alias("source_crs"),
            F.count(F.lit(1)).alias("companies"),
            F.sum(F.when(F.col("nace_division") == "03", 1).otherwise(0)).alias("fishing_companies"),
        )
    )


def geocode_locations(locations: DataFrame, ref: DataFrame, cache: DataFrame | None = None,
                      towns: DataFrame | None = None) -> DataFrame:
    """Pure function: locations + gazetteer (+ optional external cache) -> locations with lat/lon."""
    pc = postcode_centroids(ref).select(
        "country_code", "postal_code", F.col("lat").alias("pc_lat"), F.col("lon").alias("pc_lon"))
    district = gb_district_centroids(ref).select(
        F.lit("GB").alias("_dcc"), "district", F.col("lat").alias("d_lat"), F.col("lon").alias("d_lon"))
    ie_keys = ref.where(F.col("country_code") == "IE").groupBy("postal_code").agg(
        F.avg("latitude").alias("r_lat"), F.avg("longitude").alias("r_lon")).withColumnRenamed("postal_code", "routing")
    places = place_centroids(ref, towns).select(
        F.col("country_code").alias("p_cc"), "place_key", F.col("lat").alias("c_lat"), F.col("lon").alias("c_lon"))

    loc = locations.withColumns({
        "_place_key": norm_place(F.col("city")),
        "_district": F.when(F.col("country_code") == "GB", F.split("postal_code", " ").getItem(0)),
        "_routing": F.when(F.col("country_code") == "IE", F.substring("postal_code", 1, 3)),
    })
    joined = (
        loc.join(pc, ["country_code", "postal_code"], "left")
        .join(F.broadcast(district), (F.col("_district") == F.col("district")), "left")
        .join(F.broadcast(ie_keys), F.col("_routing") == F.col("routing"), "left")
        .join(places, (F.col("country_code") == F.col("p_cc")) & (F.col("_place_key") == F.col("place_key")), "left")
    )
    if cache is not None:
        joined = joined.join(
            cache.select("location_key", F.col("lat").alias("x_lat"), F.col("lon").alias("x_lon"),
                         F.col("provider").alias("x_provider")),
            "location_key", "left")
    else:
        joined = joined.withColumns({"x_lat": F.lit(None).cast("double"), "x_lon": F.lit(None).cast("double"),
                                     "x_provider": F.lit(None).cast("string")})

    src_lon, src_lat = lambert93_to_wgs84(F.col("source_x"), F.col("source_y"))
    src_lon = F.when(F.col("source_crs") == "EPSG:2154", src_lon)
    src_lat = F.when(F.col("source_crs") == "EPSG:2154", src_lat)

    cc = F.col("country_code")
    tiers = [  # (lat, lon, precision, source)
        (F.col("x_lat"), F.col("x_lon"), "ADDRESS", F.col("x_provider")),
        (src_lat, src_lon, "ADDRESS", F.lit("REGISTER_COORDINATES")),
        (F.col("pc_lat"), F.col("pc_lon"), "POSTCODE", F.lit("GEONAMES_POSTCODE")),
        (F.col("d_lat"), F.col("d_lon"), "DISTRICT", F.lit("GEONAMES_POSTCODE_DISTRICT")),
        (F.col("r_lat"), F.col("r_lon"), "DISTRICT", F.lit("GEONAMES_EIRCODE_ROUTING_KEY")),
        (F.col("c_lat"), F.col("c_lon"), "CITY", F.lit("GEONAMES_PLACE")),
    ]
    candidates = [
        F.when(lat.isNotNull() & lon.isNotNull() & _in_country(cc, lon, lat),
               F.struct(lat.alias("lat"), lon.alias("lon"), F.lit(prec).alias("precision"), src.alias("source")))
        for lat, lon, prec, src in tiers
    ]
    best = F.coalesce(*candidates)
    return joined.select(
        "location_key", "country_code", "address_line", "postal_code", "city", "municipality_code",
        "companies", "fishing_companies",
        best["lat"].alias("latitude"),
        best["lon"].alias("longitude"),
        F.coalesce(best["precision"], F.lit("NONE")).alias("geo_precision"),
        best["source"].alias("geo_source"),
    )


def ensure_cache_table(spark: SparkSession, table: str) -> None:
    create_table_if_missing(
        spark, table,
        "location_key STRING NOT NULL, lat DOUBLE, lon DOUBLE, provider STRING, matched_address STRING, "
        "geocoded_at TIMESTAMP",
        comment="Address-level results from the gradual geocoding worker (insert-only cache)",
        constraints={"lat_range": "lat IS NULL OR lat BETWEEN -90 AND 90",
                     "lon_range": "lon IS NULL OR lon BETWEEN -180 AND 180"},
    )


def ingest_worker_results(spark: SparkSession, settings: Settings) -> int:
    """Load result files written by the geocoding Lambda into the cache (insert-only MERGE)."""
    cache_table = settings.table("silver", "geocode_cache")
    ensure_cache_table(spark, cache_table)
    path = f"{settings.landing_root}/_GEOCODE/results"
    try:
        results = spark.read.schema(
            "location_key STRING, lat DOUBLE, lon DOUBLE, provider STRING, matched_address STRING"
        ).json(path)
    except Exception as exc:  # no results folder yet
        if "PATH_NOT_FOUND" in str(exc) or "does not exist" in str(exc):
            return 0
        raise
    new = results.where(F.col("location_key").isNotNull()).dropDuplicates(["location_key"]).withColumn(
        "geocoded_at", F.current_timestamp())
    from delta.tables import DeltaTable

    (
        DeltaTable.forName(spark, cache_table).alias("t")
        .merge(new.alias("s"), "t.location_key = s.location_key")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return spark.table(cache_table).count()


def export_requests(located: DataFrame, settings: Settings, batch_size: int) -> int:
    """Pick the next batch for the precise geocoder: coarse locations with a street address,
    fishing first (the map's focus), then by number of companies at the location."""
    batch = (
        located.where((F.col("geo_precision") != "ADDRESS") & F.col("address_line").isNotNull())
        .orderBy(F.col("fishing_companies").desc(), F.col("companies").desc(), F.col("location_key"))
        .limit(batch_size)
        .select("location_key", "country_code", "address_line", "postal_code", "city")
    )
    out = f"{settings.landing_root}/_GEOCODE/requests/run_id={settings.run_id}"
    batch.coalesce(1).write.mode("overwrite").json(out)  # spark-lint: ok[SPK011] bounded batch, one file for one Lambda
    return batch.count()


def worker_result_files(spark: SparkSession, settings: Settings) -> list:
    from registry_lakehouse.bronze.landing import list_landing_files

    return list_landing_files(spark, settings.landing_root, "_GEOCODE/results")


def geo_fingerprint(spark: SparkSession, settings: Settings) -> str:
    from registry_lakehouse.ops import freshness

    return freshness.fingerprint({
        "tables": freshness.data_versions(spark, [settings.table("silver", t) for t in
                                                  ("company", "ref_postal_code", "ref_place")]),
        "worker_results": freshness.files_fingerprint(worker_result_files(spark, settings)),
    })


def build_locations(spark: SparkSession, settings: Settings, request_batch_size: int = 1000) -> dict[str, int] | None:
    """Rebuild silver.location, unless companies, gazetteers and worker results are all unchanged (None)."""
    from registry_lakehouse.ops import freshness

    ensure_schema(spark, settings.catalog, settings.silver_schema)
    fp = geo_fingerprint(spark, settings)
    inputs = [settings.table("silver", t) for t in ("company", "ref_postal_code", "ref_place")]
    if freshness.unchanged(spark, settings, "geo", fp, outputs=[settings.table("silver", "location")],
                           inputs=inputs if not worker_result_files(spark, settings) else None):
        return None
    ingest_worker_results(spark, settings)
    company = spark.table(settings.table("silver", "company")).where("is_current")
    ref = spark.table(reference_table(settings))
    cache_table = settings.table("silver", "geocode_cache")
    cache = spark.table(cache_table) if table_exists(spark, cache_table) else None
    towns = spark.table(places_table(settings)) if table_exists(spark, places_table(settings)) else None
    located = geocode_locations(distinct_locations(company), ref, cache, towns)
    table = settings.table("silver", "location")
    (
        located.withColumn("_updated_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    by_precision = spark.table(table).groupBy("geo_precision").count()
    stats = {r.geo_precision: r["count"] for r in by_precision.collect()}  # spark-lint: ok[SPK001] 5 rows
    if request_batch_size:
        stats["requests_exported"] = export_requests(spark.table(table), settings, request_batch_size)
    log.info("geocoding precision: %s", stats)
    freshness.record(spark, settings, "geo", fp)
    from registry_lakehouse.ops.detect import remember_worker_results

    remember_worker_results(spark, settings)
    return stats
