"""Lambda logic, tested without AWS: URL resolution, skip/guard rules, NDJSON conversion, geocoding."""

import datetime as dt
import gzip
import json

import pytest

from registry_lakehouse.ingestion import downloader as d
from registry_lakehouse.ingestion import geocode_worker as g
from registry_lakehouse.ingestion.norway_ndjson import convert_json_array_to_ndjson_parts
from tests.conftest import FIXTURES, ROOT


# ---------------------------------------------------------------------------- requests
def test_request_files_have_no_bucket_and_valid_resolvers():
    files = sorted((ROOT / "infra" / "requests").glob("*.json"))
    assert len(files) == 6
    for f in files:
        spec = json.loads(f.read_text())
        assert "bucket" not in spec, f"{f.name}: bucket must come from the environment"
        r = d.Request.from_dict(spec)
        assert r.resolver in {"static", "template", "datagouv", "ckan"}
        for url in (r.url, r.url_template, r.api_base):
            assert url is None or url.startswith("https://"), f.name


def test_reference_requests_land_one_folder_per_source():
    files = sorted((ROOT / "infra" / "requests" / "reference").glob("*.json"))
    specs = [json.loads(f.read_text()) for f in files]
    countries = [s["country"] for s in specs]
    assert len(countries) == len(set(countries)) == 7  # one prefix each: the size guard compares like with like
    assert all(c.startswith(("REFERENCE/geonames/", "REFERENCE/geonames_cities/")) for c in countries)
    assert all(s["url"].startswith("https://download.geonames.org/") for s in specs)


def test_sam_template_schedules_every_request():
    template = (ROOT / "infra" / "template.yaml").read_text()
    requests = [*(ROOT / "infra" / "requests").glob("*.json"), *(ROOT / "infra" / "requests" / "reference").glob("*.json")]
    for f in requests:
        assert f'"country": "{json.loads(f.read_text())["country"]}"' in template.replace('"country":"', '"country": "'), f.name


def test_template_resolver_uses_first_of_month():
    r = d.Request(country="UNITED_KINGDOM", target_filename="x.csv", resolver="template",
                  url_template="https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-{first_of_month}.zip")
    assert d.resolve_url(r, dt.date(2026, 10, 13)).endswith("-2026-10-01.zip")


def test_datagouv_resolver_picks_latest_matching_resource(monkeypatch):
    meta = {"resources": [
        {"title": "Sirene : Fichier StockEtablissement - 01 septembre 2026", "format": "zip", "url": "https://a/old.zip",
         "last_modified": "2026-09-01T00:00:00"},
        {"title": "Sirene : Fichier StockEtablissement - 01 octobre 2026", "format": "zip", "url": "https://a/new.zip",
         "last_modified": "2026-10-01T00:00:00"},
        {"title": "Sirene : Fichier StockEtablissementHistorique", "format": "zip", "url": "https://a/histo.zip",
         "last_modified": "2026-10-02T00:00:00"},
    ]}
    monkeypatch.setattr(d, "_get_json", lambda url: meta)
    r = d.Request(country="FRANCE", target_filename="x", resolver="datagouv", dataset="sirene",
                  resource_title_contains="Fichier StockEtablissement - ", resource_format="zip")
    assert d.resolve_url(r, dt.date(2026, 10, 13)) == "https://a/new.zip"


def test_unchanged_download_is_skipped_and_truncated_one_refused():
    prev = {"sha256": "abc", "bytes": 1_000_000}
    assert d.should_skip(prev, 1_000_000, "abc", 0.5) == "unchanged since previous ingest"
    assert d.should_skip(prev, 900_000, "def", 0.5) is None
    with pytest.raises(RuntimeError, match="truncated"):
        d.should_skip(prev, 100_000, "def", 0.5)
    assert d.should_skip(None, 1, "x", 0.5) is None


def test_download_refuses_plain_http(tmp_path):
    with pytest.raises(ValueError, match="https"):
        d.download("http://example.invalid/file.zip", str(tmp_path / "f"))


# ---------------------------------------------------------------------------- Norway NDJSON
def test_json_array_becomes_ndjson_parts_without_pii(tmp_path):
    raw = FIXTURES / "raw" / "NORWAY" / "enheter_sample.json.gz"
    source = json.loads(gzip.open(raw, "rt", encoding="utf-8").read())
    parts = list(convert_json_array_to_ndjson_parts(str(raw), str(tmp_path), records_per_part=10))
    assert len(parts) == -(-len(source) // 10)
    records = [json.loads(line) for p in parts for line in gzip.open(p, "rt", encoding="utf-8")]
    assert [r["organisasjonsnummer"] for r in records] == [r["organisasjonsnummer"] for r in source]
    assert not any(k in r for r in records for k in ("epostadresse", "telefon", "mobil", "hjemmeside"))
    assert any("telefon" in r or "epostadresse" in r for r in source)  # the fixture does contain them


# ---------------------------------------------------------------------------- geocoding worker
def fake_fetch(url: str):
    if "geonorge" in url:
        return {"adresser": [{"adressetekst": "Strandgata 21", "representasjonspunkt": {"lat": 58.858, "lon": 5.737}}]}
    if "nominatim" in url:
        return [{"lat": "10.0", "lon": "10.0", "display_name": "somewhere in Africa"}]  # wrong country
    if "cartociudad" in url:
        return [{"lat": 42.34, "lng": -2.22, "postalCode": "26144", "address": "CALLE MAYOR 12, Corera"}]
    raise AssertionError(url)


def test_worker_validates_results_and_rate_limits():
    sleeps: list[float] = []
    reqs = [
        {"location_key": "no1", "country_code": "NO", "address_line": "STRANDGATA 21", "postal_code": "4307", "city": "SANDNES"},
        {"location_key": "gb1", "country_code": "GB", "address_line": "1 HIGH ST", "postal_code": "AB1 2CD", "city": "X"},
        {"location_key": "gb2", "country_code": "GB", "address_line": "2 HIGH ST", "postal_code": "AB1 2CD", "city": "X"},
        # CartoCiudad answers with a different town (real behaviour): the postcode check rejects it
        {"location_key": "es1", "country_code": "ES", "address_line": "CALLE MAYOR 1", "postal_code": "31001", "city": "PAMPLONA"},
        {"location_key": "fr1", "country_code": "FR", "address_line": "x", "postal_code": "1", "city": "x"},
    ]
    out = g.geocode_batch(reqs, fetch=fake_fetch, sleep=sleeps.append)
    assert [o["location_key"] for o in out] == ["no1"]
    assert out[0]["provider"] == "KARTVERKET"
    assert any(s > 0.5 for s in sleeps)  # Nominatim's 1 request/second policy is respected


def test_worker_stops_before_lambda_timeout():
    reqs = [{"location_key": f"n{i}", "country_code": "NO", "address_line": "A", "postal_code": "4307", "city": "S"}
            for i in range(5)]
    budget = iter([100, 100, 5, 5, 5])
    out = g.geocode_batch(reqs, fetch=fake_fetch, time_left_s=lambda: next(budget), sleep=lambda s: None)
    assert len(out) == 2


def test_worker_survives_provider_errors():
    def boom(url):
        raise TimeoutError("provider down")

    reqs = [{"location_key": "n", "country_code": "NO", "address_line": "A", "postal_code": "4307", "city": "S"}]
    assert g.geocode_batch(reqs, fetch=boom, sleep=lambda s: None) == []


class FakeS3:
    def __init__(self, objects):
        import datetime as _dt
        self.objects = {k: (v, _dt.datetime(2026, 10, i + 1)) for i, (k, v) in enumerate(objects.items())}
        self.put = {}

    def get_paginator(self, _name):
        outer = self

        class P:
            def paginate(self, Bucket, Prefix):
                return [{"Contents": [{"Key": k, "LastModified": t} for k, (_, t) in outer.objects.items()
                                      if k.startswith(Prefix)]}]
        return P()

    def get_object(self, Bucket, Key):
        import io as _io
        return {"Body": _io.BytesIO(self.objects[Key][0].encode())}

    def put_object(self, Bucket, Key, Body):
        self.put[Key] = Body


def test_scheduled_sweep_processes_only_unanswered_request_files(monkeypatch):
    p = "lake/landing/_GEOCODE"
    req = json.dumps({"location_key": "k", "country_code": "NO", "address_line": "A", "postal_code": "4307", "city": "S"})
    s3 = FakeS3({
        f"{p}/requests/run_id=1/part-0.json": req,
        f"{p}/results/run_id=1/part-0.json": "",      # already answered
        f"{p}/requests/run_id=2/part-0.json": req,
        f"{p}/requests/run_id=2/_SUCCESS": "",         # Spark marker, not a request
    })
    assert g.pending_requests(s3, "b", "lake/landing") == [f"{p}/requests/run_id=2/part-0.json"]
    calls = []
    monkeypatch.setattr(g, "_fetch_json", lambda url: calls.append(url) or fake_fetch(url))  # no network in tests
    monkeypatch.setattr(g.time, "sleep", lambda s: None)
    out = g.process(s3, "b", f"{p}/requests/run_id=2/part-0.json", lambda: 1e9)
    assert out["geocoded"] == 1 and f"{p}/results/run_id=2/part-0.json" in s3.put
    assert len(calls) == 1 and "geonorge" in calls[0]
