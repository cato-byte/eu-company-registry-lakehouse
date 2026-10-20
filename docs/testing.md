# Testing strategy

Databricks compute costs money and a laptop doesn't, so anything that can be tested locally is.
The local stack (PySpark 4.0, delta-spark 4.0, ANSI mode on) matches serverless environment 4.

## Layers

| Layer | Files | What it proves |
|---|---|---|
| Pure Python | `test_schema_drift.py`, `test_ingestion.py`, `test_cost.py` | drift policies, URL resolvers, skip/truncation guards, NDJSON conversion, geocoding worker (rate limit, validation, timeout budget), estimator |
| Column functions | `test_parsers.py`, `test_classify.py`, `test_lambert93.py` | each parsing rule against real-world values. Checksum expressions are property-tested with Hypothesis against a plain Python reference; Lambert-93 agrees with PROJ to 1 cm |
| Per-register mapping | `test_sources.py` | the real bronze loader on fixtures, then one test per register pinning its cleaning decisions |
| Engines | `test_quality.py`, `test_scd2.py`, `test_geocode.py`, `test_gold.py` | quarantine, thresholds and volume checks; the SCD2 lifecycle (idempotency, ordering, constraints, time travel); geocoding tier priority; gold aggregates and the privacy guard |
| Code quality | `test_code_quality.py` | the PySpark linter is clean on `src/` and catches each anti-pattern. No source plan contains a Python UDF or a cartesian product, and each stays under a size budget |
| End to end | `integration/test_pipeline_e2e.py` | landing → publish for all six registers, a second snapshot (SCD2 history), an idempotent re-run, Change Data Feed on `silver.company`, small published files without personal names |
| Privacy | `test_privacy.py` | no secrets or infrastructure identifiers in the repo, and no identifier or name shared between fixtures and the real samples |

## Fixtures are raw files

Fixtures stay in each register's native format: CSV with its own delimiter, BOM, CRLF and quoting,
and NDJSON for Norway. They go through the same bronze loader as production. Converting them to
JSON or building DataFrames by hand would skip the parsing step, and that is where most of the
real bugs were (a BOM in the first header, `;` delimiters, malformed lines). Hand-built DataFrames
are only used for focused unit tests of a single function.

The fixtures are synthetic but shaped on real samples (see `docs/privacy.md`). They include the
edge cases on purpose: fishing companies in every register that codes activity, sole traders,
closed, insolvent and liquidating units, foreign addresses, `[ND]`, `1900-01-01`, float codes,
mojibake, an invalid SIRET, a malformed line, a duplicate row, a future date and an unexpected
JSON key. A second UK snapshot adds a liquidation, a move, a removal and a new company.

## Catching Spark mistakes before Databricks does

`tools/spark_lint.py` runs in pre-commit, in CI and as a test. It flags code that works locally
but fails or costs money on serverless:

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

Two bugs it caught during the refactor:

* Composing `clean_text → parse_int → nace_from_numeric` copied each input expression several
  times per level, and building the Spanish plan ran the driver out of memory. A `let` binding
  (`transform(array(x), ...)`) fixed it, and the plan-size test keeps it fixed.
* On Spark 4, `encode/decode` threw on values such as `SÃO PAULO`, which broke the mojibake repair.
  A regex now admits only strings whose round trip cannot fail.

## Running

```bash
make test        # unit tests, about 5-10 min on local Spark
make e2e         # end-to-end pipeline
pytest -k norway # any subset
```
