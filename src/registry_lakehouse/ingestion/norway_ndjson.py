"""Stream Brønnøysund's one-big-JSON-array export into gzipped NDJSON parts.

Spark can only read a top-level JSON array with ``multiLine=true``, which puts the whole 2.8 GB
document in a single task on a single core: slow, and a memory risk on serverless. Rewriting it
once at ingestion (ijson streams it, so memory stays flat) lets Spark split the work across
parts and cores. PII keys are removed here, so they never reach S3 at all.
"""

from __future__ import annotations

import gzip
import json
import os
from collections.abc import Iterator
from decimal import Decimal
from typing import IO

DROP_KEYS = ("epostadresse", "telefon", "mobil", "hjemmeside")


def _open(path: str) -> IO[bytes]:
    with open(path, "rb") as probe:
        magic = probe.read(2)
    return gzip.open(path, "rb") if magic == b"\x1f\x8b" else open(path, "rb")


def _json_default(o: object) -> object:
    if isinstance(o, Decimal):
        return int(o) if o == o.to_integral_value() else float(o)
    raise TypeError(type(o))


def iter_records(path: str) -> Iterator[dict]:
    import ijson

    with _open(path) as fh:
        yield from ijson.items(fh, "item")


def convert_json_array_to_ndjson_parts(
    src: str, out_dir: str, records_per_part: int = 250_000, drop_keys: tuple[str, ...] = DROP_KEYS
) -> Iterator[str]:
    os.makedirs(out_dir, exist_ok=True)
    part, n, out = 0, 0, None
    try:
        for rec in iter_records(src):
            if out is None:
                path = os.path.join(out_dir, f"part-{part:05d}.ndjson.gz")
                out = gzip.open(path, "wt", encoding="utf-8")  # noqa: SIM115  # spans several yields
            for k in drop_keys:
                rec.pop(k, None)
            out.write(json.dumps(rec, ensure_ascii=False, default=_json_default))
            out.write("\n")
            n += 1
            if n % records_per_part == 0:
                out.close()
                out = None
                yield path
                part += 1
        if out is not None:
            out.close()
            out = None
            yield path
    finally:
        if out is not None:
            out.close()
