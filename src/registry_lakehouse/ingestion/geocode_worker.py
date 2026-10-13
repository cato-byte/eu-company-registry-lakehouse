"""AWS Lambda: gradually geocode a batch of addresses with free providers.

Triggered by the request file the pipeline writes to <landing>/_GEOCODE/requests/. Results go to
<landing>/_GEOCODE/results/, and the next pipeline run merges them into silver.geocode_cache, so
no address is ever geocoded twice.

Providers are free public services, preferring each country's own government geocoder:
  NO  Kartverket adresse-API (ws.geonorge.no)
  EE  Maa-amet In-ADS gazetteer
  ES  IGN CartoCiudad (result accepted only if its postcode matches ours)
  FR  not needed: the register publishes coordinates
  IE, GB  OpenStreetMap Nominatim: max 1 request/s, identifying User-Agent, results cached, as
          the usage policy requires. For large volumes, self-host Nominatim or Photon.

Every result is validated (country bounding box, plus a postcode cross-check where the provider
returns one) before it is written. The worker stops cleanly before Lambda's timeout, and the
rest is picked up by the next batch.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USER_AGENT = os.environ.get(
    "GEOCODER_USER_AGENT", "registry-lakehouse/0.2 (+https://github.com/cato-byte/eu-company-registry-lakehouse)")

COUNTRY_BBOX = {
    "GB": (-8.7, 49.8, 1.9, 60.95), "IE": (-10.7, 51.3, -5.9, 55.5), "EE": (21.7, 57.4, 28.3, 59.8),
    "ES": (-2.6, 41.85, -0.6, 43.35),  # Navarra only
    "NO": (4.0, 57.9, 31.3, 71.3), "FR": (-5.5, 41.2, 9.8, 51.2),
}


@dataclass(frozen=True)
class GeoResult:
    lat: float
    lon: float
    matched_address: str
    provider: str


Fetch = Callable[[str], Any]


def _fetch_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:  # fixed https endpoints
        text = resp.read().decode("utf-8")
    if text.startswith("callback("):  # CartoCiudad JSONP
        text = text[len("callback("):-1]
    return json.loads(text)


def kartverket(req: dict, fetch: Fetch) -> GeoResult | None:
    q = urllib.parse.urlencode({"sok": req["address_line"], "postnummer": req.get("postal_code") or "",
                                "treffPerSide": 1, "utkoordsys": 4258})
    data = fetch(f"https://ws.geonorge.no/adresser/v1/sok?{q}")
    hits = data.get("adresser") or []
    if not hits:
        return None
    pt = hits[0]["representasjonspunkt"]
    return GeoResult(pt["lat"], pt["lon"], hits[0].get("adressetekst", ""), "KARTVERKET")


def in_ads(req: dict, fetch: Fetch) -> GeoResult | None:
    q = urllib.parse.urlencode({"address": f"{req['address_line']} {req.get('city') or ''}", "results": 1})
    data = fetch(f"https://inaadress.maaamet.ee/inaadress/gazetteer?{q}")
    hits = data.get("addresses") or []
    if not hits or not hits[0].get("viitepunkt_b"):
        return None
    h = hits[0]
    return GeoResult(float(h["viitepunkt_b"]), float(h["viitepunkt_l"]), h.get("taisaadress", ""), "MAAAMET_INADS")


def cartociudad(req: dict, fetch: Fetch) -> GeoResult | None:
    q = urllib.parse.urlencode({"q": f"{req['address_line']}, {req.get('city') or ''}", "limit": 1})
    data = fetch(f"https://www.cartociudad.es/geocoder/api/geocoder/candidatesJsonp?{q}")
    if not data:
        return None
    h = data[0]
    # Free-text search can land in a different town with the same street name: require the postcode to match.
    if req.get("postal_code") and h.get("postalCode") != req["postal_code"]:
        return None
    return GeoResult(float(h["lat"]), float(h["lng"]), h.get("address", ""), "IGN_CARTOCIUDAD")


def nominatim(req: dict, fetch: Fetch) -> GeoResult | None:
    q = urllib.parse.urlencode({
        "street": req["address_line"], "postalcode": req.get("postal_code") or "", "city": req.get("city") or "",
        "countrycodes": req["country_code"].lower(), "format": "jsonv2", "limit": 1})
    data = fetch(f"https://nominatim.openstreetmap.org/search?{q}")
    if not data:
        return None
    return GeoResult(float(data[0]["lat"]), float(data[0]["lon"]), data[0].get("display_name", ""), "OSM_NOMINATIM")


PROVIDERS: dict[str, tuple[Callable[[dict, Fetch], GeoResult | None], float]] = {
    # country: (provider, minimum seconds between calls)
    "NO": (kartverket, 0.2),
    "EE": (in_ads, 0.2),
    "ES": (cartociudad, 0.2),
    "IE": (nominatim, 1.1),
    "GB": (nominatim, 1.1),
}


def valid(country: str, r: GeoResult) -> bool:
    x0, y0, x1, y1 = COUNTRY_BBOX[country]
    return x0 <= r.lon <= x1 and y0 <= r.lat <= y1


def geocode_batch(requests: list[dict], fetch: Fetch = _fetch_json,
                  time_left_s: Callable[[], float] = lambda: 1e9, sleep: Callable[[float], None] = time.sleep,
                  ) -> list[dict]:
    out: list[dict] = []
    last_call: dict[str, float] = {}
    for req in requests:
        if time_left_s() < 20:
            logger.info("stopping early: Lambda time budget nearly used, %d left for next batch",
                        len(requests) - len(out))
            break
        provider = PROVIDERS.get(req["country_code"])
        if provider is None:
            continue
        fn, min_gap = provider
        wait = min_gap - (time.monotonic() - last_call.get(fn.__name__, 0.0))
        if wait > 0:
            sleep(wait)
        last_call[fn.__name__] = time.monotonic()
        try:
            res = fn(req, fetch)
        except Exception as exc:  # one bad address or a provider hiccup must not kill the batch
            logger.warning("geocode failed for %s: %s", req["location_key"], exc)
            continue
        if res is not None and valid(req["country_code"], res):
            out.append({"location_key": req["location_key"], "lat": res.lat, "lon": res.lon,
                        "provider": res.provider, "matched_address": res.matched_address})
    return out


def lambda_handler(event: dict, context: Any = None) -> dict:
    import boto3

    s3 = boto3.client("s3")
    rec = event["Records"][0]["s3"]
    bucket, key = rec["bucket"]["name"], urllib.parse.unquote_plus(rec["object"]["key"])
    lines = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8").splitlines()
    requests = [json.loads(line) for line in lines if line.strip()]
    remaining = (lambda: context.get_remaining_time_in_millis() / 1000) if context else (lambda: 1e9)
    results = geocode_batch(requests, time_left_s=remaining)
    out_key = key.replace("/_GEOCODE/requests/", "/_GEOCODE/results/")
    s3.put_object(Bucket=bucket, Key=out_key, Body="\n".join(json.dumps(r) for r in results).encode("utf-8"))
    logger.info("geocoded %d/%d -> s3://%s/%s", len(results), len(requests), bucket, out_key)
    return {"requested": len(requests), "geocoded": len(results), "results_key": out_key}
