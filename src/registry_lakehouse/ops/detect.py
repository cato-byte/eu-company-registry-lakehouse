"""First task of the job: decide whether anything needs to run at all.

Compares the newest landing snapshot of every register, the reference files and the geocoding
worker's result files with what the pipeline last processed. The answer is passed to the
Databricks job as task values:

    run        "true" / "false"   -> a condition task skips the whole job when "false"
    countries  JSON list          -> the bronze/silver for-each loops run only for these

When only reference or geocoding inputs changed, every country is listed: bronze and silver then
skip themselves in seconds, and geo/gold/publish recompute.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from pyspark.sql import SparkSession

from registry_lakehouse.bronze.landing import list_landing_files, pick_snapshot
from registry_lakehouse.config import Settings
from registry_lakehouse.contracts import COUNTRY_CODES, get_contract
from registry_lakehouse.geo import geocode, reference
from registry_lakehouse.ops import freshness
from registry_lakehouse.silver.sources import MAPPERS
from registry_lakehouse.spark import on_databricks

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Changes:
    countries: tuple[str, ...]
    reference: bool
    geocoding: bool
    first_run: bool

    @property
    def run(self) -> bool:
        return bool(self.countries or self.reference or self.geocoding or self.first_run)

    @property
    def countries_to_process(self) -> list[str]:
        if self.countries:
            return list(self.countries)
        return list(COUNTRY_CODES) if self.run else []


def detect_changes(spark: SparkSession, settings: Settings) -> Changes:
    changed = []
    for cc in COUNTRY_CODES:
        try:
            snapshot = pick_snapshot(list_landing_files(spark, settings.landing_root, get_contract(cc).country),
                                     cc, settings.snapshot_date)
        except FileNotFoundError:
            log.warning("[%s] no landing snapshot", cc)
            continue
        landed = freshness.files_fingerprint(list(snapshot.files))
        key_bumped = (freshness.last_fingerprint(spark, settings, f"silver_key:{cc}") or "1") != \
            str(MAPPERS[cc].key_version)
        if key_bumped or freshness.last_fingerprint(spark, settings, f"bronze:{cc}") != landed:
            changed.append(cc)
    try:
        ref_fp = freshness.fingerprint(reference.reference_inputs(spark, settings))
        ref_changed = freshness.last_fingerprint(spark, settings, "reference") != ref_fp
    except FileNotFoundError:
        ref_changed = False
    results = geocode.worker_result_files(spark, settings)
    geo_changed = bool(results) and \
        freshness.last_fingerprint(spark, settings, "worker_results") != freshness.files_fingerprint(results)
    first_run = freshness.last_fingerprint(spark, settings, "publish") is None
    changes = Changes(tuple(changed), ref_changed, geo_changed, first_run)
    log.info("changes: %s -> run=%s countries=%s", changes, changes.run, changes.countries_to_process)
    return changes


def remember_worker_results(spark: SparkSession, settings: Settings) -> None:
    """Called after geo consumed the worker results, so the same files do not trigger another run."""
    freshness.record(spark, settings, "worker_results",
                     freshness.files_fingerprint(geocode.worker_result_files(spark, settings)))


def publish_task_values(changes: Changes) -> None:
    """Hand the decision to the Databricks job (task values). Locally it is only logged."""
    values = {"run": "true" if changes.run else "false", "countries": changes.countries_to_process}
    if on_databricks():
        from databricks.sdk.runtime import dbutils  # type: ignore[import-not-found]

        for key, value in values.items():
            dbutils.jobs.taskValues.set(key=key, value=value)
    log.info("task values: %s", json.dumps(values))
