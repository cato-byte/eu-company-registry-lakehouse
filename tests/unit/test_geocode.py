"""Tiered geocoding: tier priority, bounding-box validation, place-name normalisation."""

import pytest
from pyspark.sql import functions as F

from registry_lakehouse.geo.geocode import geocode_locations
from registry_lakehouse.geo.reference import norm_place

pytestmark = pytest.mark.spark

REF_COLS = "country_code string, postal_code string, place_key string, place_name string, region string, " \
           "municipality string, latitude double, longitude double"
LOC_COLS = "location_key string, country_code string, address_line string, postal_code string, city string, " \
           "municipality_code string, source_x double, source_y double, source_crs string, companies long, " \
           "fishing_companies long"


@pytest.fixture
def ref(spark):
    rows = [
        ("GB", "AB43 7AN", "FRASERBURGH", "Fraserburgh", "Scotland", None, 57.69, -2.01),
        ("GB", "AB43 7AP", "FRASERBURGH", "Fraserburgh", "Scotland", None, 57.70, -2.02),
        ("IE", "F92", "LETTERKENNY", "Letterkenny", "Ulster", None, 54.95, -7.73),
        ("NO", "4307", "SANDNES", "Sandnes", "Rogaland", "Sandnes", 58.85, 5.73),
        ("EE", "10115", "TALLINN", "Tallinn", "Harju", "Tallinn", 59.43, 24.75),
        ("ES", "31007", "PAMPLONA", "Pamplona", "Navarra", "Pamplona/Iruña", 42.81, -1.65),
        ("FR", "84000", "AVIGNON", "Avignon", "PACA", "Avignon", 43.95, 4.81),
        ("NO", "9999", "BOGUS", "Bogus", "x", None, 10.0, 10.0),  # outside Norway: must be rejected
    ]
    return spark.createDataFrame(rows, REF_COLS)


def locate(spark, ref, rows, cache=None):
    locs = spark.createDataFrame(rows, LOC_COLS)
    return {r.location_key: (r.geo_precision, r.geo_source, r.latitude, r.longitude)
            for r in geocode_locations(locs, ref, cache).collect()}


def test_tiers_in_priority_order(spark, ref):
    res = locate(spark, ref, [
        ("fr-lambert", "FR", "RUE X", "84000", "AVIGNON", None, 851150.1, 6317267.1, "EPSG:2154", 1, 0),
        ("gb-full", "GB", "1 HIGH ST", "AB43 7AN", "FRASERBURGH", None, None, None, None, 1, 1),
        ("gb-district", "GB", "1 HIGH ST", "AB43 9ZZ", "NOWHERE", None, None, None, None, 1, 0),
        ("ie-routing", "IE", "MAIN ST", "F92 X2Y3", "SOMEWHERE", None, None, None, None, 1, 0),
        ("ee-city", "EE", "Kase tn 1", None, "TALLINN", None, None, None, None, 1, 0),
        ("es-postcode", "ES", "CALLE MAYOR 1", "31007", "PAMPLONA", None, None, None, None, 1, 0),
        ("none", "ES", None, None, "ATLANTIS", None, None, None, None, 1, 0),
    ])
    assert res["fr-lambert"][:2] == ("ADDRESS", "REGISTER_COORDINATES")
    assert abs(res["fr-lambert"][2] - 43.95) < 0.05  # converted, lands in Avignon
    assert res["gb-full"][:2] == ("POSTCODE", "GEONAMES_POSTCODE")
    assert res["gb-district"][:2] == ("DISTRICT", "GEONAMES_POSTCODE_DISTRICT")
    assert res["ie-routing"][:2] == ("DISTRICT", "GEONAMES_EIRCODE_ROUTING_KEY")
    assert res["ee-city"][:2] == ("CITY", "GEONAMES_PLACE")
    assert res["es-postcode"][:2] == ("POSTCODE", "GEONAMES_POSTCODE")
    assert res["none"] == ("NONE", None, None, None)


def test_out_of_country_candidates_are_rejected(spark, ref):
    res = locate(spark, ref, [("bad", "NO", None, "9999", "BOGUS", None, None, None, None, 1, 0)])
    assert res["bad"][0] == "NONE"


def test_external_cache_wins_over_everything(spark, ref):
    cache = spark.createDataFrame([("gb-full", 57.6912, -2.0051, "OSM_NOMINATIM")],
                                  "location_key string, lat double, lon double, provider string")
    res = locate(spark, ref, [("gb-full", "GB", "1 HIGH ST", "AB43 7AN", "FRASERBURGH", None, None, None, None, 1, 1)],
                 cache)
    assert res["gb-full"][:2] == ("ADDRESS", "OSM_NOMINATIM")


def test_place_normalisation(eval_col):
    assert eval_col(lambda c: norm_place(F.col(c)), ["Saint-Étienne", " SAINT  ETIENNE ", "Bærum", "Pamplona / Iruña"]) \
        == ["SAINT ETIENNE", "SAINT ETIENNE", "BARUM", "PAMPLONA IRUNA"]


def test_town_tier_prefers_most_populous_namesake(spark, ref):
    towns = spark.createDataFrame(
        [("IE", "KILLYBEGS", 54.633, -8.45, 1236), ("IE", "NEWTOWN", 52.0, -7.0, 600),
         ("IE", "NEWTOWN", 53.5, -6.5, 9000)],
        "country_code string, place_key string, latitude double, longitude double, population long")
    locs = spark.createDataFrame([
        ("k", "IE", "MAIN ST", None, "KILLYBEGS", None, None, None, None, 1, 1),
        ("n", "IE", "MAIN ST", None, "NEWTOWN", None, None, None, None, 1, 0),
    ], LOC_COLS)
    res = {r.location_key: r for r in geocode_locations(locs, ref, None, towns).collect()}
    assert res["k"].geo_precision == "CITY" and abs(res["k"].latitude - 54.633) < 1e-6
    assert res["n"].latitude == 53.5  # the town of 9,000, not an average of the two
