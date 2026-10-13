"""Lambert-93 (EPSG:2154) -> WGS84 lon/lat as native Spark expressions.

INSEE publishes Lambert-93 coordinates for most metropolitan establishments, which gives
address-level geocoding for free. The inverse Lambert Conformal Conic projection is
computed column-wise (closed form plus a fixed-point iteration for latitude) instead of in
a pyproj UDF: no Python serialisation per row, and no native dependency on serverless.
RGF93 and WGS84 differ by well under a metre, so no datum shift is applied. Accuracy against
pyproj is checked in the tests.
"""

from __future__ import annotations

import math

from pyspark.sql import Column
from pyspark.sql import functions as F

N = 0.7256077650532670
C = 11754255.4260960
XS = 700000.0
YS = 12655612.0498760
E = 0.0818191910428158  # GRS80 first eccentricity
LON0 = math.radians(3.0)
ITERATIONS = 6  # converges to < 1e-11 rad, i.e. sub-millimetre

# Metropolitan France + Corsica bounding box (WGS84), used to reject bad conversions.
FR_BBOX = (-5.5, 41.2, 9.8, 51.2)


def lambert93_to_wgs84(x: Column, y: Column) -> tuple[Column, Column]:
    dx = x - F.lit(XS)
    dy = y - F.lit(YS)
    r = F.sqrt(dx * dx + dy * dy)
    gamma = F.atan2(dx, -dy)
    lon = F.lit(LON0) + gamma / F.lit(N)
    iso_lat = -F.log(r / F.lit(C)) / F.lit(N)
    phi0 = F.lit(2.0) * F.atan(F.exp(iso_lat)) - F.lit(math.pi / 2)

    def step(phi: Column, _: Column) -> Column:
        esin = F.lit(E) * F.sin(phi)
        return F.lit(2.0) * F.atan(F.pow((1 + esin) / (1 - esin), F.lit(E / 2)) * F.exp(iso_lat)) - F.lit(math.pi / 2)

    # A higher-order `aggregate` keeps the plan small. Unrolling the loop in Python would double
    # the expression tree on every iteration.
    phi = F.aggregate(F.sequence(F.lit(1), F.lit(ITERATIONS)), phi0, step)
    return F.degrees(lon), F.degrees(phi)


def in_bbox(lon: Column, lat: Column, bbox: tuple[float, float, float, float]) -> Column:
    min_lon, min_lat, max_lon, max_lat = bbox
    return lon.between(min_lon, max_lon) & lat.between(min_lat, max_lat)
