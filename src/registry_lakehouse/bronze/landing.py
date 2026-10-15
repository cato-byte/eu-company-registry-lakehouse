"""Locate landing files for a source and snapshot.

Layout written by the ingestion Lambda:

    <landing_root>/<COUNTRY>/ingest_date=YYYY-MM-DD/<file(s)>

Files placed directly under <COUNTRY>/ (the original manual layout) are treated as one
undated snapshot. Listing goes through Spark's ``binaryFile`` source (only path/size/mtime
are selected, so file contents are never read). That works identically on a laptop, on
S3 and on Unity Catalog volumes, and needs no ``dbutils`` or JVM access, which serverless
(Spark Connect) does not expose.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

_SNAPSHOT_RE = re.compile(r"ingest_date=(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class LandingFile:
    path: str
    size_bytes: int
    snapshot_date: date | None


@dataclass(frozen=True)
class LandingSnapshot:
    country: str
    snapshot_date: date | None
    files: tuple[LandingFile, ...]

    @property
    def total_bytes(self) -> int:
        return sum(f.size_bytes for f in self.files)

    @property
    def paths(self) -> list[str]:
        return [f.path for f in self.files]


def list_landing_files(spark: SparkSession, landing_root: str, country: str) -> list[LandingFile]:
    base = f"{landing_root.rstrip('/')}/{country}"
    # The whole read is inside the try: classic Spark reports a missing folder at load(), but
    # Spark Connect (serverless) is lazy and only reports it when the rows are fetched.
    try:
        rows = (
            spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(base)
            .select("path", "length")
            .where(~F.col("path").rlike(r"/(_|\.)[^/]*$"))  # skip _SUCCESS, .crc, hidden files
            .collect()  # spark-lint: ok[SPK001] file listing: one row per file, never data rows
        )
    except Exception as exc:  # folder does not exist (yet)
        if "PATH_NOT_FOUND" in str(exc) or "does not exist" in str(exc):
            return []
        raise
    files = []
    for r in rows:
        m = _SNAPSHOT_RE.search(r.path)
        files.append(LandingFile(r.path, int(r.length), date.fromisoformat(m.group(1)) if m else None))
    return sorted(files, key=lambda f: f.path)


def latest_reference_files(spark: SparkSession, landing_root: str, folder: str, suffix: str = ".txt") -> list[str]:
    """Latest download of every reference file under <landing_root>/<folder>.

    Reference dumps are landed one per source (REFERENCE/geonames/FR/ingest_date=.../FR.txt), on
    different days. Taking the newest ingest_date per source, instead of the newest folder overall,
    keeps every country present. GeoNames' bundled readme.txt is skipped.
    """
    latest: dict[str, LandingFile] = {}
    for f in list_landing_files(spark, landing_root, folder):
        if not f.path.endswith(suffix) or f.path.rsplit("/", 1)[-1].lower().startswith("readme"):
            continue
        source = f.path.split("/ingest_date=")[0] if f.snapshot_date else f.path
        best = latest.get(source)
        if best is None or (f.snapshot_date or date.min) > (best.snapshot_date or date.min):
            latest[source] = f
    if not latest:
        raise FileNotFoundError(f"No reference files under {landing_root}/{folder}")
    return sorted(f.path for f in latest.values())


def pick_snapshot(files: list[LandingFile], country: str, as_of: date | None = None) -> LandingSnapshot:
    """Latest dated snapshot on or before ``as_of``; falls back to undated files."""
    if not files:
        raise FileNotFoundError(f"No landing files for {country}")
    dated = sorted({f.snapshot_date for f in files if f.snapshot_date is not None})
    if as_of is not None:
        dated = [d for d in dated if d <= as_of]
    if dated:
        chosen = dated[-1]
        return LandingSnapshot(country, chosen, tuple(f for f in files if f.snapshot_date == chosen))
    undated = tuple(f for f in files if f.snapshot_date is None)
    if not undated:
        raise FileNotFoundError(f"No landing snapshot for {country} on or before {as_of}")
    return LandingSnapshot(country, None, undated)
