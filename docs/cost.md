# Cost: predicting and controlling Databricks serverless spend

## How serverless jobs are billed

Serverless job compute is billed in **DBUs consumed**, at your workspace's price per DBU for the
serverless jobs SKU (it depends on cloud, region, plan and contract). There is no cluster to size,
so cost is driven by **how much data each task scans and shuffles, and for how long**.

## The estimator (`registry-cost`)

```bash
registry-cost estimate --sizes FR=9.3GB,GB=2.6GB,IE=184MB,NO=2.8GB,EE=92MB,ES=69MB --usd-per-dbu <your price>
registry-cost estimate --landing s3://<bucket>/<prefix>/landing      # reads object sizes
registry-cost report-sql                                            # query for real costs per run
registry-cost calibrate history.csv > coefficients.json              # fit DBU/GB per stage
registry-cost estimate --sizes ... --coefficients coefficients.json  # calibrated prediction
```

Model: `DBU(stage, country) = fixed overhead + k_stage × input GB × format factor`.

The shipped numbers are assumptions, not measurements. `k_stage` is set deliberately high, NDJSON
gets a format factor of 1.6 against 1.0 for CSV (JSON parsing is slower; the exact ratio is a
guess), and the reported band is ×0.4-2.0 of the point estimate. After calibration the band
narrows to ×0.7-1.4, which is also a chosen width rather than a fitted interval.

After 3-5 production runs, export the result of `report-sql` together with input sizes
(`stage,input_gb,dbus` CSV) and run `calibrate`. From then on the estimate is grounded in the
workspace's own history. The `cost_report` task writes the same query result to `ops.cost_by_run`
after every run, so the history builds up automatically.

## What keeps the bill small

* **Skip unchanged inputs.** The Lambda skips files whose SHA-256 matches the previous ingest. The
  monthly job starts with a change gate: on unchanged data it ends after one small task, and only
  registers with a new snapshot are reprocessed (see `docs/operations.md`).
* **Incremental silver.** Unchanged companies produce no writes (hash_diff). Only new versions are written.
* **No geocoding API calls inside Spark.** Offline joins, plus a free external worker.
* **Read once.** The annotated snapshot is materialised to a staging Delta table (serverless has no
  `.cache()`), so data quality, quarantine and MERGE don't each re-parse bronze.
* **Splittable inputs.** Norway as NDJSON parts. CSV without `multiLine`.
* **No Python UDFs** (linter + plan tests): native expressions stay in the JVM/Photon.
* **`performance_target: STANDARD`** on the job: serverless cost-optimised mode.
* **Guard rails** stop runaways early: input size guard, task timeouts, duration alerts.
* **Budget policy** (optional): attach a serverless budget policy to the job to tag and cap spend
  per project in `system.billing.usage`.

## Local first

Every code path runs on a laptop against the fixtures (`make test-all`). The only reason to run on
Databricks is production volume.
