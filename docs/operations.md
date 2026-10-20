# Operations: scheduling, alerting, guard rails

## Scheduling

| What | Where | When |
|---|---|---|
| Register downloads | EventBridge Scheduler → `DownloaderFunction` (`infra/template.yaml`) | monthly, before the pipeline run on the 6th: FR 2nd, GB/IE 3rd, EE/NO 4th, ES 5th (yearly data, skipped when unchanged), GeoNames 1st |
| Lakehouse job | Databricks job `registry-lakehouse-<target>` (`resources/registry_job.yml`) | monthly, the 6th at 04:00 Europe/Paris, after the monthly downloads (paused in `dev`) |
| Precise geocoding | `GeocodeWorkerFunction`, EventBridge Scheduler | daily at 06:00 UTC, processes request files without results |

A file-arrival trigger on the landing location is the alternative to the cron schedule (it is
commented out in the job). The downloader writes `_manifest.json` last and skips unchanged files by
checksum, so with that trigger the job would only run when a register actually changed.

### Skipping unchanged data

The first task, `detect_changes`, compares each register's newest landing snapshot (file paths and
sizes), the GeoNames files and the geocoding results with what was last processed. If nothing
changed, the condition task `has_changes` ends the run after that one small task. If only some
registers changed, the bronze and silver loops run for those alone.

Every stage also checks its own inputs (`ops.stage_state`, see `ops/freshness.py`) and skips when
they are unchanged. Maintenance commits such as `OPTIMIZE` or `VACUUM` don't count as changes. If
the state table is lost, existing results are adopted, not recomputed: bronze when it already holds
the same files, geo and gold when their outputs are newer than their inputs. The end-to-end test
`test_rerun_with_unchanged_inputs_detects_nothing_and_writes_nothing` asserts that a re-run on the
same files produces no new data version in any table and rewrites no published file.

### Job shape

```
detect_changes ─► has_changes? ─ false ─► run ends
                       │ true
                       ├─► bronze (for_each changed registers, 3 at a time) ─► silver (for_each, 1 at a time) ─┐
                       └─► reference (GeoNames gazetteer) ──────────────────────────────────────────────────────┴─► geo ─► gold ─► publish ─► cost_report (run_if ALL_DONE)
```

Silver runs one country at a time because every country merges into the same `silver.company`
table, and concurrent MERGEs into one table conflict.

`for_each_task` runs one task per country, so a failure in Norway shows up as Norway's task and
can be repaired and re-run on its own (bronze and silver are idempotent per snapshot).

## Guard rails

| Risk | Guard |
|---|---|
| A task hangs or crawls | `timeout_seconds` per task (15-60 min) and per job (3 h): Databricks cancels it |
| A run is slow but not yet timed out | health rule `RUN_DURATION_SECONDS > 5400` → `on_duration_warning_threshold_exceeded` e-mail |
| Runs pile up | `max_concurrent_runs: 1` |
| An unexpectedly huge input file (cost runaway) | `max_landing_bytes` check before reading (default 20 GB). The run refuses to start |
| A truncated download wipes half the companies | Lambda refuses files < 50% of the previous size. Silver aborts if a snapshot has < 80% of the previous row count |
| Bad data silently propagates | DQ rules with abort thresholds. ERROR rows and unparseable source lines to quarantine |
| Upstream schema change | drift check before reading (strict/additive policy per source) |
| A code change silently reshapes a gold table | gold compares each table's schema before overwriting it and sends a `WARN` alert listing added, removed and retyped columns |
| Retrying a data problem burns money | `max_retries: 0` on silver. Bronze retries once (transient I/O only) |
| Memory | Serverless does not expose memory settings, and an OOM fails the task (then alerts). The code avoids the usual causes: no `collect()` of data (linter SPK001), no unbounded plans (plan-size tests + `let` bindings), no single-task reads (Norway NDJSON, no `multiLine`) |

## Alerting

* Failed and slow runs send job e-mail notifications (`alert_email` bundle variable). A
  `webhook_notifications` block can route them to Slack or Teams once a notification destination
  exists in the workspace.
* Non-fatal findings (additive schema drift, malformed lines, quarantined rows, gold schema
  changes) go through `ops/alerts.py` to the webhook stored as secret `alert-webhook` in the job's
  secret scope. Delivery is best effort and never fails the run.
* Lambda errors trigger a CloudWatch alarm (`Errors > 0`) that e-mails through SNS.
* `ops.dq_results` keeps the data-quality history, one row per rule, country, snapshot and run.
  With a SQL warehouse, a Databricks SQL alert on `breached = 'true'` or a rising `failure_rate`
  can sit on top of it.

### Monitoring the tables themselves

The checks above run inside the job. Databricks data quality monitoring (formerly Lakehouse
Monitoring) watches the tables from outside: freshness and completeness on a schema, or profile and
drift metrics on one table. It is not set up yet. The natural first candidates are
`silver.company`, which would catch a register that stops updating, and `gold.agg_city_industry`,
which feeds the map. It runs on serverless and is billed separately from the job.

## Delta Lake features in use

| Feature | Where | Why |
|---|---|---|
| `MERGE` with `WHEN NOT MATCHED BY SOURCE` | silver SCD2 | one atomic statement inserts new versions, closes changed ones, and closes companies that left the register |
| Change Data Feed | `silver.company` | row-level changes per snapshot (new, changed, closed companies) for incremental readers. Tested in e2e |
| Time travel / `RESTORE` | silver | reproduce any past state. Reload history after a bug fix (the loader refuses out-of-order snapshots and says to RESTORE) |
| CHECK constraints | `silver.company`, `geocode_cache` | the table rejects invalid states (unknown status, open versions not current, malformed NACE) even if code regresses |
| Liquid clustering | bronze (`_snapshot_date`), silver (`country_code, nace_division`) | file skipping for the per-country MERGE and the map queries, with no partition tuning |
| `delta.dataSkippingStatsColumns` | bronze, silver | stats only on filtered columns (Delta indexes the first 32 columns by default, and clustering keys must have stats) |
| Deletion vectors | all | cheap MERGE updates (no full file rewrites) |
| Predictive optimization | all (catalog level, `setup.sql`) | OPTIMIZE, VACUUM and compaction run when Databricks predicts they pay off. No per-table autoOptimize settings, no scheduled maintenance job |
| `replaceWhere` | bronze, quarantine, stage | idempotent re-runs of a snapshot |
| Column comments | silver | the canonical model documents itself in Unity Catalog |

## Recovering

* To re-run one country, use *Repair run* on the failed run and select only that country's task.
  Bronze and silver are idempotent, so a repeat is safe.
* If a bad snapshot was merged, `RESTORE TABLE <catalog>.silver.company TO VERSION AS OF <n>`, fix
  the code and re-run. `DESCRIBE HISTORY` shows which run (`_run_id`) wrote each version.
* Rejected rows and unparseable lines are in quarantine:
  `SELECT failures, record FROM <catalog>.silver.quarantine WHERE country_code = 'FR'`.
