"""Lambert-93 -> WGS84 in pure Spark expressions, checked against PROJ (pyproj)."""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import functions as F

from registry_lakehouse.geo.lambert93 import FR_BBOX, in_bbox, lambert93_to_wgs84

pytestmark = pytest.mark.spark
pyproj = pytest.importorskip("pyproj")
TO_WGS84 = pyproj.Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)


def _convert(spark, points):
    df = spark.createDataFrame(points, "x double, y double")
    lon, lat = lambert93_to_wgs84(F.col("x"), F.col("y"))
    return df.select("x", "y", lon.alias("lon"), lat.alias("lat")).collect()


def test_known_point_paris(spark):
    # Notre-Dame de Paris, approximately (652 000, 6 862 000) in Lambert-93
    [row] = _convert(spark, [(652000.0, 6862000.0)])
    lon, lat = TO_WGS84.transform(652000.0, 6862000.0)
    assert abs(row.lon - lon) < 1e-7 and abs(row.lat - lat) < 1e-7
    assert 2.3 < row.lon < 2.4 and 48.8 < row.lat < 48.9


@settings(max_examples=10, deadline=None)
@given(st.lists(st.tuples(st.floats(100_000, 1_200_000), st.floats(6_050_000, 7_120_000)), min_size=20, max_size=20))
def test_matches_proj_everywhere_in_france(spark, points):
    """Sub-centimetre agreement (1e-7 degrees is about 1 cm) over metropolitan France."""
    for row in _convert(spark, points):
        lon, lat = TO_WGS84.transform(row.x, row.y)
        assert abs(row.lon - lon) < 1e-7, (row.x, row.y)
        assert abs(row.lat - lat) < 1e-7, (row.x, row.y)


def test_overseas_coordinates_fall_outside_metropolitan_bbox(spark):
    # Mayotte publishes UTM 38S values in the same columns; read as Lambert-93 they land far away.
    [row] = _convert(spark, [(530468.9, 8587048.2)])
    df = spark.createDataFrame([(row.lon, row.lat)], "lon double, lat double")
    assert df.select(in_bbox(F.col("lon"), F.col("lat"), FR_BBOX)).first()[0] is False
