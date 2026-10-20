"""Runtime settings.

Nothing environment-specific (bucket names, account ids, webhook URLs) lives in the code.
Values come from job parameters (Databricks Asset Bundle) or environment variables, with
safe local defaults so the whole pipeline runs on a laptop against the synthetic fixtures.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from datetime import date


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    # Where raw files land: s3://<bucket>/.../landing, /Volumes/<cat>/<schema>/landing or a local dir.
    landing_root: str
    # Unity Catalog catalog on Databricks; "spark_catalog" locally (Delta session catalog).
    catalog: str = "spark_catalog"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    ops_schema: str = "ops"
    # Where the static website assets are written (local dir or s3:// prefix).
    publish_root: str = "site/data"
    # Guard rails: refuse to process a landing file bigger than this (protects against runaway cost).
    max_landing_bytes: int = 20 * 1024**3
    # Data-quality: share of rows a FATAL rule may fail before the run is aborted.
    dq_fatal_threshold: float = 0.05
    # Data-quality: share of source lines that may be malformed (unsplittable) before the run aborts.
    max_malformed_rate: float = 0.01
    # Failure-rate thresholds are statistically meaningless on tiny snapshots.
    dq_min_rows_for_rate: int = 1000
    # Insights: an address with at least this many active companies is a mass-registration address.
    mass_address_min_companies: int = 50
    # Insights: towns need at least this many fishing companies to be ranked as a fishing hub.
    fishing_hub_min_companies: int = 3
    # Volume anomaly: abort if a snapshot has fewer rows than this share of the previous one.
    min_volume_ratio: float = 0.8
    # Alerting: name of the env var / Databricks secret holding a Slack/Teams webhook URL.
    alert_webhook_env: str = "REGISTRY_ALERT_WEBHOOK"
    snapshot_date: date = field(default_factory=date.today)
    run_id: str = "local"

    def table(self, layer: str, name: str) -> str:
        schema = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
            "ops": self.ops_schema,
        }[layer]
        return f"{self.catalog}.{schema}.{name}"

    def with_overrides(self, **kwargs: object) -> Settings:
        return replace(self, **{k: v for k, v in kwargs.items() if v is not None})


def settings_from_env() -> Settings:
    landing_root = _env("REGISTRY_LANDING_ROOT", "tests/fixtures/landing")
    assert landing_root is not None
    snapshot = _env("REGISTRY_SNAPSHOT_DATE")
    return Settings(
        landing_root=landing_root.rstrip("/"),
        catalog=_env("REGISTRY_CATALOG", "spark_catalog") or "spark_catalog",
        publish_root=_env("REGISTRY_PUBLISH_ROOT", "site/data") or "site/data",
        max_landing_bytes=int(_env("REGISTRY_MAX_LANDING_BYTES", str(20 * 1024**3)) or 0),
        snapshot_date=date.fromisoformat(snapshot) if snapshot else date.today(),
        run_id=_env("REGISTRY_RUN_ID", "local") or "local",
        mass_address_min_companies=int(_env("REGISTRY_MASS_ADDRESS_MIN", "50") or 50),
        fishing_hub_min_companies=int(_env("REGISTRY_FISHING_HUB_MIN", "3") or 3),
    )
