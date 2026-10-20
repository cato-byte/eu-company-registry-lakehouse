# ADR 0004: Lakeflow Jobs running a Python wheel, not Lakeflow pipelines

**Status:** accepted, to be revisited (see the end)

## Context

Databricks recommends Lakeflow pipelines (Spark Declarative Pipelines) for most new ETL. Several
parts of this project overlap with what a pipeline provides out of the box:

| Here | In a Lakeflow pipeline |
|---|---|
| SCD2 `MERGE` from full register snapshots (`silver/scd2.py`) | `AUTO CDC FROM SNAPSHOT`, `stored_as_scd_type = 2` |
| Data-quality rules, quarantine, abort thresholds (`quality/`) | Expectations: `expect`, `expect_or_drop`, `expect_or_fail` |
| Gold tables rebuilt with `overwrite` | Materialized views, refreshed incrementally |
| Change gate and per-stage fingerprints (`ops/detect.py`, `ops/freshness.py`) | Dependency tracking and incremental refresh |
| File listing and fingerprints in bronze | Auto Loader with its checkpoint, `_rescued_data`, `schemaEvolutionMode` |

So the choice needs a reason.

## Decision

Keep a Lakeflow Job with `python_wheel_task`s on serverless, with explicit Spark and Delta code.

1. **The whole pipeline runs on a laptop.** The local stack is PySpark 4.0 with delta-spark,
   matching serverless environment 4. The end-to-end test runs landing → publish on the fixtures in
   a few minutes for free. Declarative pipelines are not part of that stack, so pipeline code
   could only be tested on a workspace, on paid compute.
2. **The quality rules need rate thresholds.** A register always has a few bad rows. The rule we
   need is "quarantine the bad rows, and abort only when more than 1% fail". `expect_or_fail`
   stops on the first violating row, and `expect_or_drop` discards rows instead of keeping them for
   replay. Both can be worked around with extra tables and checks, but by then the engine is
   rebuilt in another form.
3. **A snapshot is the unit of work.** Registers publish full dumps. Bronze replaces one snapshot
   at a time (`replaceWhere`), and the contract check runs on the header before any data is read.
   Auto Loader processes files incrementally and remembers them in a checkpoint. That fits
   appended files well, but reloading one snapshot after a parser fix would mean resetting that
   state.

`AUTO CDC FROM SNAPSHOT` would fit silver well. It is not a reason to stay with jobs. The reasons
are the three points above.

## Consequences

* We maintain the change gate ourselves: `detect_changes`, `ops.stage_state`, and the retry of
  stages left unfinished by a failed run. A pipeline would get this from its dependency graph.
* Gold is rebuilt table by table, so a failure in the middle leaves gold partly refreshed until
  the next run. The gate re-runs the unfinished stage, and schema changes in gold raise an alert.
* Expectations, pipeline event logs and lineage views in the pipeline UI are not used. Unity
  Catalog still records table lineage for the job.

## When to revisit

* when the local test stack moves to a Spark release that ships declarative pipelines, or
* when the change gate needs another fix.

The cheapest first step would be gold as materialized views: it is a pure function of silver, and
it would make the gold refresh atomic. Silver with `AUTO CDC FROM SNAPSHOT` would come second.
