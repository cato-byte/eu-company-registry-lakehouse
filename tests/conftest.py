"""Shared test fixtures: one local Delta-enabled SparkSession (ANSI on, like serverless)."""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Callable, Iterator
from datetime import date
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
LANDING = FIXTURES / "landing"


@pytest.fixture(scope="session")
def spark() -> Iterator[Any]:
    warehouse = tempfile.mkdtemp(prefix="registry-test-warehouse-")
    os.environ["REGISTRY_LOCAL_WAREHOUSE"] = warehouse
    os.environ.setdefault("REGISTRY_PSEUDONYM_KEY", "test-pepper")
    from registry_lakehouse.spark import get_spark

    session = get_spark("registry-tests", warehouse)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def eval_col(spark) -> Callable[..., list]:
    """Evaluate a Column-building function over a list of input values in ONE Spark job.

    eval_col(parse_int, ["5.0", "x"]) -> [5, None]
    """

    def run(fn: Callable, values: list, schema: str = "v string") -> list:
        df = spark.createDataFrame([(v,) for v in values], schema)
        return [r[0] for r in df.select(fn("v").alias("out")).collect()]

    return run


@pytest.fixture
def make_settings(tmp_path) -> Callable[..., Any]:
    from registry_lakehouse.config import Settings

    def build(prefix: str, **overrides: Any) -> Settings:
        base = dict(
            landing_root=str(LANDING),
            bronze_schema=f"{prefix}_bronze", silver_schema=f"{prefix}_silver",
            gold_schema=f"{prefix}_gold", ops_schema=f"{prefix}_ops",
            publish_root=str(tmp_path / "publish"), snapshot_date=date(2026, 7, 31), run_id=f"test-{prefix}",
        )
        base.update(overrides)
        return Settings(**base)

    return build
