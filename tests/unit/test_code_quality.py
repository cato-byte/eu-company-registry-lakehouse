"""Catch coding errors before they reach (paid) Databricks compute.

1. Static: the PySpark anti-pattern linter must be clean on src/, and must actually detect
   each anti-pattern (tested on known-bad snippets).
2. Plans: every source transformation compiles to a plan with no Python UDF, no cartesian
   product and a bounded size. Plan blow-ups, such as an unrolled loop of column expressions,
   are caught here instead of as a driver OOM in production.
"""

import io
import re
import textwrap
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path

import pytest

from tests.conftest import ROOT
from tools.spark_lint import RULES, lint, lint_file


def test_source_tree_is_lint_clean():
    findings = lint([str(ROOT / "src")])
    assert not findings, "\n" + "\n".join(map(str, findings))


BAD_SNIPPETS = {
    "SPK001": "rows = df.collect()",
    "SPK002": "@udf('string')\ndef f(x):\n    return x",
    "SPK003": "n = df.rdd.getNumPartitions()",
    "SPK004": "df.cache()",
    "SPK005": "spark.read.option('inferSchema', 'true').csv(p)",
    "SPK006": "for c in cols:\n    df = df.withColumn(c, F.lit(1))",
    "SPK007": "a.crossJoin(b)",
    "SPK008": "spark.conf.set('spark.sql.shuffle.partitions', 8)",
    "SPK009": "F.to_date(F.col('d'), 'dd/MM/yyyy')",
    "SPK010": "PATH = 's3://my-real-bucket/landing'",
    "SPK011": "df.repartition(1).write.save(p)",
}


@pytest.mark.parametrize(("code", "snippet"), BAD_SNIPPETS.items())
def test_linter_detects_each_antipattern(tmp_path, code, snippet):
    f = tmp_path / "bad.py"
    f.write_text(textwrap.dedent(snippet))
    assert code in {x.code for x in lint_file(f)}


def test_linter_suppression_requires_a_reason(tmp_path):
    ok = tmp_path / "ok.py"
    ok.write_text("rows = df.collect()  # spark-lint: ok[SPK001] three rows at most\n")
    bare = tmp_path / "bare.py"
    bare.write_text("rows = df.collect()  # spark-lint: ok[SPK001]\n")
    assert lint_file(ok) == [] and len(lint_file(bare)) == 1


def test_every_rule_has_a_snippet():
    assert set(BAD_SNIPPETS) == set(RULES)


# ---------------------------------------------------------------------------- plans
FORBIDDEN_IN_PLAN = ["BatchEvalPython", "ArrowEvalPython", "CartesianProduct", "BroadcastNestedLoopJoin"]
PLAN_BUDGET_CHARS = 400_000


def plan_of(df) -> str:
    """Plan text with expression ids removed (they grow with the session, not with the plan)."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(mode="extended")
    return re.sub(r"#\d+L?", "#", buf.getvalue())


@pytest.mark.spark
@pytest.mark.parametrize("cc", ["FR", "GB", "IE", "EE", "ES", "NO"])
def test_source_plans_have_no_python_udfs_or_cartesian_products(spark, cc):
    from registry_lakehouse.contracts import get_contract
    from registry_lakehouse.quality import engine
    from registry_lakehouse.quality.rules import rules_for
    from registry_lakehouse.silver.build import canonical_snapshot, deduplicate

    contract = get_contract(cc)
    cols = ", ".join(f"`{c}` string" for c in contract.bronze_columns)
    empty = spark.createDataFrame([], f"{cols}, _snapshot_date date, _corrupt_record string")
    snap = date(2022, 12, 31) if cc == "ES" else date(2026, 7, 1)
    df = deduplicate(engine.annotate(canonical_snapshot(empty, cc, snap), rules_for(cc)))
    plan = plan_of(df)
    for bad in FORBIDDEN_IN_PLAN:
        assert bad not in plan, f"{cc}: {bad} in plan"
    assert len(plan) < PLAN_BUDGET_CHARS, f"{cc}: plan is {len(plan):,} chars (expression blow-up?)"


@pytest.mark.spark
def test_geocode_plan_has_no_udf_and_bounded_size(spark):
    from registry_lakehouse.geo.geocode import geocode_locations
    from tests.unit.test_geocode import LOC_COLS, REF_COLS

    plan = plan_of(geocode_locations(spark.createDataFrame([], LOC_COLS), spark.createDataFrame([], REF_COLS)))
    for bad in FORBIDDEN_IN_PLAN:
        assert bad not in plan
    assert len(plan) < PLAN_BUDGET_CHARS


def test_no_notebook_style_globals_in_package():
    """Modules must not create sessions or read data at import time (the old notebooks did)."""
    for path in Path(ROOT / "src").rglob("*.py"):
        text = path.read_text()
        assert "getOrCreate()" not in text or path.name == "spark.py", path
