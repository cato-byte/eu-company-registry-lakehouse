"""AWS Lambda: download one register export and land it in S3, date-partitioned.

    s3://<bucket>/<prefix>/<COUNTRY>/ingest_date=YYYY-MM-DD/<file>
    s3://<bucket>/<prefix>/<COUNTRY>/ingest_date=YYYY-MM-DD/_manifest.json

Improvements over the original script:
* URL resolvers: registers that publish a new URL each month (data.gouv.fr, CKAN portals,
  dated Companies House files) are resolved at run time, with no manual edits.
* Unchanged files are skipped: the SHA-256 is compared with the previous manifest, so the
  Databricks job (file-arrival trigger) does not run, and cost nothing, when nothing changed.
* Norway's single 2.8 GB JSON array is rewritten as gzipped NDJSON parts, so Spark reads it in
  parallel instead of in one task (see norway_ndjson.py).
* Plausibility guard: a file far smaller than the previous one is not landed (truncated download).

The bucket comes from the environment (TARGET_S3_BUCKET), never from the request files.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USER_AGENT = "registry-lakehouse/0.2 (+https://github.com/cato-byte/eu-company-registry-lakehouse)"
TMP = os.environ.get("TMP_DIR", "/tmp")  # noqa: S108  # Lambda's only writable path
CHUNK = 1024 * 1024


@dataclass(frozen=True)
class Request:
    country: str
    target_filename: str
    resolver: str  # static | template | datagouv | ckan
    url: str | None = None
    url_template: str | None = None
    dataset: str | None = None
    resource_title_contains: str | None = None
    resource_format: str | None = None
    api_base: str | None = None
    transform: str | None = None  # e.g. "json_array_to_ndjson"
    min_ratio_vs_previous: float = 0.5

    @staticmethod
    def from_dict(d: dict[str, Any]) -> Request:
        allowed = set(Request.__dataclass_fields__)
        return Request(**{k: v for k, v in d.items() if k in allowed})


# ----------------------------------------------------------------------------------------
# URL resolution
# ----------------------------------------------------------------------------------------
def _get_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:  # https URLs from reviewed config
        return json.loads(resp.read().decode("utf-8"))


def resolve_url(r: Request, today: dt.date) -> str:
    if r.resolver == "static":
        assert r.url
        return r.url
    if r.resolver == "template":
        assert r.url_template
        return r.url_template.format(first_of_month=today.replace(day=1).isoformat(), today=today.isoformat())
    if r.resolver == "datagouv":
        meta = _get_json(f"https://www.data.gouv.fr/api/1/datasets/{r.dataset}/")
        candidates = [res for res in meta["resources"]
                      if (r.resource_title_contains or "") in res["title"]
                      and (not r.resource_format or res["format"] == r.resource_format)]
        if not candidates:
            raise LookupError(f"no data.gouv resource matching {r.resource_title_contains!r}")
        return max(candidates, key=lambda res: res.get("last_modified") or "")["url"]
    if r.resolver == "ckan":
        meta = _get_json(f"{r.api_base}/api/3/action/package_show?id={urllib.parse.quote(r.dataset or '')}")
        for res in meta["result"]["resources"]:
            if not r.resource_format or res.get("format", "").upper() == r.resource_format.upper():
                return res["url"]
        raise LookupError(f"no CKAN resource in {r.dataset}")
    raise ValueError(f"unknown resolver {r.resolver}")


# ----------------------------------------------------------------------------------------
# Download + land
# ----------------------------------------------------------------------------------------
def download(url: str, dest: str) -> tuple[int, str]:
    if not url.startswith("https://"):
        raise ValueError(f"refusing non-https download: {url}")
    sha = hashlib.sha256()
    size = 0
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=300) as resp, open(dest, "wb") as fh:
        while chunk := resp.read(CHUNK):
            fh.write(chunk)
            sha.update(chunk)
            size += len(chunk)
    return size, sha.hexdigest()


def previous_manifest(s3: Any, bucket: str, country_prefix: str) -> dict | None:
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{country_prefix}/ingest_date=")
    keys = sorted(o["Key"] for p in pages for o in p.get("Contents", []) if o["Key"].endswith("_manifest.json"))
    if not keys:
        return None
    body = s3.get_object(Bucket=bucket, Key=keys[-1])["Body"].read()
    return json.loads(body)


def should_skip(prev: dict | None, size: int, sha256: str, min_ratio: float) -> str | None:
    if prev is None:
        return None
    if prev.get("sha256") == sha256:
        return "unchanged since previous ingest"
    if prev.get("bytes") and size < prev["bytes"] * min_ratio:
        raise RuntimeError(f"download is {size:,} bytes, < {min_ratio:.0%} of previous {prev['bytes']:,}: "
                           "probably truncated; not landing it")
    return None


def land(s3: Any, local_path: str, bucket: str, prefix: str, r: Request) -> list[str]:
    keys = []
    if r.transform == "json_array_to_ndjson":
        from registry_lakehouse.ingestion.norway_ndjson import convert_json_array_to_ndjson_parts

        for part in convert_json_array_to_ndjson_parts(local_path, os.path.join(TMP, "ndjson")):
            key = f"{prefix}/{os.path.basename(part)}"
            s3.upload_file(part, bucket, key)
            os.remove(part)
            keys.append(key)
    elif zipfile.is_zipfile(local_path):
        with zipfile.ZipFile(local_path) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                key = f"{prefix}/{os.path.basename(info.filename)}"
                with z.open(info) as data:  # streamed: never fully extracted to disk
                    s3.upload_fileobj(data, bucket, key)
                keys.append(key)
    else:
        key = f"{prefix}/{r.target_filename}"
        s3.upload_file(local_path, bucket, key)
        keys.append(key)
    return keys


def lambda_handler(event: dict, context: Any = None) -> dict:
    import boto3

    bucket = os.environ["TARGET_S3_BUCKET"]
    base = os.environ.get("LANDING_PREFIX", "company_records/lakehouse/landing").strip("/")
    r = Request.from_dict(event)
    today = dt.date.fromisoformat(event["ingest_date"]) if event.get("ingest_date") else dt.date.today()
    country_prefix = f"{base}/{r.country}"
    prefix = f"{country_prefix}/ingest_date={today.isoformat()}"
    s3 = boto3.client("s3")

    url = resolve_url(r, today)
    local = os.path.join(TMP, "download.bin")
    try:
        size, sha = download(url, local)
        logger.info("downloaded %s (%s bytes, sha256=%s)", url, size, sha)
        reason = should_skip(previous_manifest(s3, bucket, country_prefix), size, sha, r.min_ratio_vs_previous)
        if reason:
            logger.info("skip %s: %s", r.country, reason)
            return {"status": "skipped", "reason": reason, "country": r.country}
        keys = land(s3, local, bucket, prefix, r)
        manifest = {"country": r.country, "source_url": url, "bytes": size, "sha256": sha,
                    "files": keys, "ingested_at": dt.datetime.now(dt.UTC).isoformat()}
        # Written last: its presence marks the snapshot as complete for the Databricks trigger.
        s3.put_object(Bucket=bucket, Key=f"{prefix}/_manifest.json", Body=json.dumps(manifest).encode(),
                      ContentType="application/json")
        return {"status": "landed", **manifest}
    finally:
        if os.path.exists(local):
            os.remove(local)
