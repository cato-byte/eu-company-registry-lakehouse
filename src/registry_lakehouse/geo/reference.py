"""GeoNames postal-code reference (CC BY 4.0, https://download.geonames.org/export/zip/).

A free, offline gazetteer: postal code -> place name -> centroid for all six countries
(Ireland by Eircode routing key, the UK by full postcode). Joining against it geocodes
millions of companies to postcode or city precision in seconds, with no API calls, rate limits
or cost. Files are expected at <landing_root>/REFERENCE/geonames/*.txt (tab separated, no header).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from registry_lakehouse.bronze.landing import latest_reference_files
from registry_lakehouse.config import Settings
from registry_lakehouse.spark import ensure_schema

GEONAMES_SCHEMA = StructType([
    StructField("country_code", StringType()),
    StructField("postal_code", StringType()),
    StructField("place_name", StringType()),
    StructField("admin_name1", StringType()),
    StructField("admin_code1", StringType()),
    StructField("admin_name2", StringType()),
    StructField("admin_code2", StringType()),
    StructField("admin_name3", StringType()),
    StructField("admin_code3", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("accuracy", StringType()),
])

_ACCENTED = "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÑÒÓÔÕÖØÙÚÛÜÝŠŽ"
_PLAIN = "AAAAAAACEEEEIIIINOOOOOOUUUUYSZ"
assert len(_ACCENTED) == len(_PLAIN)


def norm_place(col: Column) -> Column:
    """'Saint-Étienne' / 'SAINT ETIENNE' / 'St. Etienne ' compare equal-ish: upper, de-accent,
    punctuation to spaces, single spaces."""
    up = F.upper(F.trim(col))
    plain = F.translate(up, _ACCENTED, _PLAIN)
    return F.trim(F.regexp_replace(F.regexp_replace(plain, r"[^A-Z0-9 ]", " "), r"\s+", " "))


CITIES_SCHEMA = StructType([
    StructField("geonameid", StringType()), StructField("name", StringType()),
    StructField("asciiname", StringType()), StructField("alternatenames", StringType()),
    StructField("latitude", DoubleType()), StructField("longitude", DoubleType()),
    StructField("feature_class", StringType()), StructField("feature_code", StringType()),
    StructField("country_code", StringType()), StructField("cc2", StringType()),
    StructField("admin1", StringType()), StructField("admin2", StringType()),
    StructField("admin3", StringType()), StructField("admin4", StringType()),
    StructField("population", StringType()), StructField("elevation", StringType()),
    StructField("dem", StringType()), StructField("timezone", StringType()),
    StructField("modified", StringType()),
])


def reference_table(settings: Settings) -> str:
    return settings.table("silver", "ref_postal_code")


def places_table(settings: Settings) -> str:
    return settings.table("silver", "ref_place")


def load_place_reference(spark: SparkSession, settings: Settings) -> int:
    """GeoNames cities500: every town of 500+ inhabitants (CC BY 4.0). It fills the gap where the
    register has no usable postcode (e.g. most Irish companies have no Eircode)."""
    raw = (
        spark.read.format("csv").schema(CITIES_SCHEMA).option("sep", "\t").option("quote", "")
        .load(latest_reference_files(spark, settings.landing_root, "REFERENCE/geonames_cities"))
    )
    names = raw.select("country_code", "name", "latitude", "longitude", "population").unionByName(
        raw.select("country_code", F.col("asciiname").alias("name"), "latitude", "longitude", "population"))
    places = names.select(
        "country_code", norm_place(F.col("name")).alias("place_key"), "latitude", "longitude",
        F.coalesce(F.col("population").try_cast("bigint"), F.lit(0)).alias("population"),
    ).where("latitude IS NOT NULL AND place_key <> ''")
    places.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        places_table(settings))
    return spark.table(places_table(settings)).count()


def load_postal_reference(spark: SparkSession, settings: Settings) -> int:
    raw = (
        spark.read.format("csv").schema(GEONAMES_SCHEMA).option("sep", "\t").option("header", "false")
        .load(latest_reference_files(spark, settings.landing_root, "REFERENCE/geonames"))
    )
    ref = raw.select(
        "country_code",
        F.upper(F.trim("postal_code")).alias("postal_code"),
        norm_place(F.col("place_name")).alias("place_key"),
        F.col("place_name"),
        F.col("admin_name1").alias("region"),
        F.col("admin_name3").alias("municipality"),
        F.col("latitude"),
        F.col("longitude"),
    ).where(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    ensure_schema(spark, settings.catalog, settings.silver_schema)
    ref.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(reference_table(settings))
    return spark.table(reference_table(settings)).count()


def postcode_centroids(ref: DataFrame) -> DataFrame:
    return ref.groupBy("country_code", "postal_code").agg(
        F.avg("latitude").alias("lat"), F.avg("longitude").alias("lon"))


def gb_district_centroids(ref: DataFrame) -> DataFrame:
    gb = ref.where(F.col("country_code") == "GB")
    return gb.groupBy(F.split("postal_code", " ").getItem(0).alias("district")).agg(
        F.avg("latitude").alias("lat"), F.avg("longitude").alias("lon"))


def place_centroids(ref: DataFrame, towns: DataFrame | None = None) -> DataFrame:
    """One point per (country, normalised place name).

    Towns (cities500) win: for a duplicated name ('NEWTOWN') the most populous one is taken, because
    averaging namesakes would land in a field between them. Postcode-file place and municipality
    names fill the rest, so 'TALLINN' matches even though Estonian postcodes are labelled by district.
    """
    by_place = ref.select("country_code", "place_key", "latitude", "longitude")
    by_muni = ref.where(F.col("municipality").isNotNull()).select(
        "country_code", norm_place(F.col("municipality")).alias("place_key"), "latitude", "longitude")
    postal = by_place.unionByName(by_muni).groupBy("country_code", "place_key").agg(
        F.avg("latitude").alias("lat"), F.avg("longitude").alias("lon"), F.lit(0).alias("rank"))
    if towns is None:
        return postal.drop("rank")
    biggest = towns.groupBy("country_code", "place_key").agg(
        F.max_by(F.struct("latitude", "longitude"), "population").alias("p"))
    town_points = biggest.select("country_code", "place_key", F.col("p.latitude").alias("lat"),
                                 F.col("p.longitude").alias("lon"), F.lit(1).alias("rank"))
    return (
        town_points.unionByName(postal)
        .groupBy("country_code", "place_key")
        .agg(F.max_by(F.struct("lat", "lon"), "rank").alias("best"))
        .select("country_code", "place_key", F.col("best.lat").alias("lat"), F.col("best.lon").alias("lon"))
    )
