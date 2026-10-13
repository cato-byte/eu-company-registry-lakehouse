# Testing strategy

Databricks compute costs money. A laptop doesn't. Everything that can be tested locally is.

## Layers

| Layer | Files | What it proves |
|---|---|---|
| Pure Python | `test_schema_drift.py`, `test_ingestion.py`, `test_cost.py` | drift policies, URL resolvers, skip/truncation guards, NDJSON conversion, geocoding worker (rate limit, validation, timeout budget), estimator |
| Column functions | `test_parsers.py`, `test_classify.py`, `test_lambert93.py` | every parsing rule against real-world values. **Property-based** (Hypothesis): Spark checksum expressions = Python reference. Lambert-93 = PROJ to 1 cm |
| Per-register mapping | `test_sources.py` | the real bronze loader on fixtures, then one test per register pinning its cleaning decisions |
| Engines | `test_quality.py`, `test_scd2.py`, `test_geocode.py`, `test_gold.py` | quarantine/threshold/volume logic. SCD2 lifecycle, idempotency, ordering, constraints, time travel. Geocoding tier priority and validation. Gold aggregates and privacy guard |
| Code quality | `test_code_quality.py` | the PySpark linter is clean on `src/` and catches each anti-pattern. Every source's plan has no Python UDF, no cartesian product, and stays under a size budget |
| End to end | `integration/test_pipeline_e2e.py` | landing → publish for all six registers, a second snapshot (SCD2 history), idempotent re-run, Change Data Feed, published files are small and free of personal names |
| Privacy | `test_privacy.py` | no secrets or infrastructure identifiers in the repo. Fixtures share no identifier or name with the real samples |

## Fixtures: why raw files, not JSON

Fixtures are kept in each register's **native format** (CSV with its own delimiter, BOM, CRLF and
quoting, NDJSON for Norway) and go through the same bronze loader as production. Converting them to
JSON or building DataFrames by hand would skip the parsing step, and that is where the real bugs
were (BOM in the first header, `;` delimiters, malformed lines). Small hand-built DataFrames are
used only for focused unit tests of one function.

The fixtures are synthetic but derived from real samples (see `docs/privacy.md`). They contain the
deliberate edge cases: fishing companies in every coded register, sole traders, closed, insolvent
and liquidating units, foreign addresses, `[ND]`, `1900-01-01`, float codes, mojibake, an invalid
SIRET, a malformed line, a duplicate row, a future date, an unexpected JSON key, and a second UK
snapshot with a liquidation, a move, a removal and a new company.

## Catching coding errors before Databricks does

`tools/spark_lint.py` (pre-commit, CI, and a test) flags patterns that work locally but fail or cost
money on serverless:

| Rule | Pattern |
|---|---|
| SPK001 | `collect()` / `toPandas()` without a justified, bounded reason |
| SPK002 | Python UDFs |
| SPK003 | RDD / SparkContext / JVM access (unavailable on Spark Connect) |
| SPK004 | `cache()` / `persist()` (unsupported on serverless) |
| SPK005 | `inferSchema`, `input_file_name()` |
| SPK006 | `withColumn` in a loop |
| SPK007 | `crossJoin` |
| SPK008 | `spark.conf.set` |
| SPK009 | `to_date`/`to_timestamp` with a format (throws under ANSI) |
| SPK010 | hard-coded `s3://` locations |
| SPK011 | `repartition(1)` / `coalesce(1)` |

Two bugs this setup caught during the refactor:

* **Exponential plan growth:** composing `clean_text → parse_int → nace_from_numeric` copied each
  input expression several times per level. Building the Spanish plan ran the driver out of memory.
  Fixed with a `let` binding (`transform(array(x), ...)`), and guarded by the plan-size test.
* **`encode/decode` throwing on Spark 4:** the mojibake repair failed on values such as `SÃO PAULO`.
  It is now guarded by a regex that admits only strings for which the round trip cannot fail.

## Running

```bash
make test        # unit tests (about 5-10 min, local Spark)
make e2e         # end-to-end pipeline
pytest -k norway # anything selectively
```
