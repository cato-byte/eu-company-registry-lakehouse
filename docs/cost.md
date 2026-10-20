# Cost: predicting and controlling Databricks serverless spend

## How serverless jobs are billed

Serverless job compute is billed in DBUs, at the workspace's price per DBU for the serverless jobs
SKU (that price depends on cloud, region, plan and contract). There is no cluster to size, so the
bill follows how much data each task scans and shuffles, and for how long.

## The estimator (`registry-cost`)

```bash
registry-cost estimate --sizes FR=9.3GB,GB=2.6GB,IE=184MB,NO=2.8GB,EE=92MB,ES=69MB --usd-per-dbu <your price>
registry-cost estimate --landing s3://<bucket>/<prefix>/landing      # reads object sizes
registry-cost report-sql                                            # query for real costs per run
registry-cost calibrate history.csv > coefficients.json              # fit DBU/GB per stage
registry-cost estimate --sizes ... --coefficients coefficients.json  # calibrated prediction
```

The model is `DBU(stage, country) = fixed overhead + k_stage × input GB × format factor`.

The shipped numbers are assumptions, not measurements. `k_stage` is set deliberately high, NDJSON
gets a format factor of 1.6 against 1.0 for CSV (JSON parsing is slower; the exact ratio is a
guess), and the reported band is ×0.4-2.0 of the point estimate. After calibration the band
narrows to ×0.7-1.4, which is also a chosen width rather than a fitted interval.

After 3-5 production runs, export the result of `report-sql` together with input sizes
(`stage,input_gb,dbus` CSV) and run `calibrate`. From then on the estimate comes from the
workspace's own history. The `cost_report` task writes the same query result to `ops.cost_by_run`
after every run, so that history builds up without manual work.

## What keeps the bill small

* Unchanged inputs are skipped. The downloader Lambda drops files whose SHA-256 matches the
  previous ingest, and the job starts with a change gate: on unchanged data it ends after one small
  task, and only registers with a new snapshot are reprocessed (see `docs/operations.md`).
* Silver is incremental. Companies whose `hash_diff` hasn't changed produce no writes.
* Spark never calls a geocoding API. Geocoding is an offline join, plus a free external worker.
* Each snapshot is parsed once. Serverless has no `.cache()`, so the annotated snapshot is written
  to a staging Delta table that data quality, quarantine and the MERGE all read.
* Inputs are splittable: Norway arrives as NDJSON parts, and CSV is read without `multiLine`.
* There are no Python UDFs (enforced by the linter and plan tests), so expressions stay in the
  JVM and Photon.
* The job runs with `performance_target: STANDARD`, the cost-optimised serverless mode.
* Guard rails stop runaways early: an input size limit, task timeouts and duration alerts.
* Optionally, a serverless budget policy on the job tags and caps its spend in
  `system.billing.usage`.

## Local first

Every code path runs on a laptop against the fixtures (`make test-all`). Databricks is only needed
for production volumes.
