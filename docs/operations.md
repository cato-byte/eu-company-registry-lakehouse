# Operations: scheduling, alerting, guard rails

## Scheduling

| What | Where | When |
|---|---|---|
| Register downloads | EventBridge Scheduler → `DownloaderFunction` (`infra/template.yaml`) | monthly, before the pipeline run on the 6th: FR 2nd, GB/IE 3rd, EE/NO 4th, ES 5th (yearly data, skipped when unchanged), GeoNames 1st |
| Lakehouse job | Databricks job `registry-lakehouse-<target>` (`resources/registry_job.yml`) | monthly, the 6th at 04:00 Europe/Paris, after the monthly downloads (paused in `dev`) |
| Precise geocoding | `GeocodeWorkerFunction`, EventBridge Scheduler | daily at 06:00 UTC, processes request files without results |

Alternative to the cron schedule: a **file-arrival trigger** on the landing location (commented in the
job). The downloader writes `_manifest.json` last and skips unchanged files by checksum, so the job then
runs only when a register really changed. That is the cheapest option.

### Unchanged data costs (almost) nothing

The first task, `detect_changes`, compares each register's newest landing snapshot (file paths and
sizes), the GeoNames files and the geocoding results with what was last processed. If nothing
changed, the condition task `has_changes` stops the run there: one small task instead of a full
pipeline. If some registers changed, the bronze and silver loops run only for those.

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

`for_each_task` runs one task per country. A failure in Norway doesn't hide behind a single
monolithic notebook, and the country can be repaired and re-run on its own (bronze and silver
are idempotent per snapshot).

## Guard rails ("stop it before it gets expensive")

| Risk | Guard |
|---|---|
| A task hangs or crawls | `timeout_seconds` per task (15-60 min) and per job (3 h): Databricks cancels it |
| A run is slow but not yet timed out | health rule `RUN_DURATION_SECONDS > 5400` → `on_duration_warning_threshold_exceeded` e-mail |
| Runs pile up | `max_concurrent_runs: 1` |
| An unexpectedly huge input file (cost runaway) | `max_landing_bytes` check before reading (default 20 GB). The run refuses to start |
| A truncated download wipes half the companies | Lambda refuses files < 50% of the previous size. Silver aborts if a snapshot has < 80% of the previous row count |
| Bad data silently propagates | DQ rules with abort thresholds. ERROR rows to quarantine |
| Upstream schema change | drift check before reading (strict/additive policy per source) |
| Retrying a data problem burns money | `max_retries: 0` on silver. Bronze retries once (transient I/O only) |
| Memory | Serverless does not expose memory settings, and an OOM fails the task (then alerts). The code avoids the usual causes: no `collect()` of data (linter SPK001), no unbounded plans (plan-size tests + `let` bindings), no single-task reads (Norway NDJSON, no `multiLine`) |

## Alerting

* **Failures and slow runs:** job e-mail notifications (`alert_email` bundle variable). A
  `webhook_notifications` block can send them to Slack or Teams once a notification destination exists.
* **Non-fatal findings** (additive schema drift, malformed lines, quarantined rows): `ops/alerts.py` posts
  to the webhook stored as secret `alert-webhook` in the job's secret scope. It is best effort and never fails the run.
* **Lambda failures:** CloudWatch alarm on `Errors > 0` → SNS e-mail.
* **Data quality history:** `ops.dq_results` (one row per rule, country, snapshot and run). A Databricks
  SQL alert on `breached = 'true'` or on rising `failure_rate` can be added if a SQL warehouse is available.

## Delta Lake features in use

| Feature | Where | Why |
|---|---|---|
| `MERGE` with `WHEN NOT MATCHED BY SOURCE` | silver SCD2 | one atomic statement inserts new versions, closes changed ones, and closes companies that left the register |
| Change Data Feed | all managed tables | downstream consumers can read only what changed. Tested in e2e |
| Time travel / `RESTORE` | silver | reproduce any past state. Reload history after a bug fix (the loader refuses out-of-order snapshots and says to RESTORE) |
| CHECK constraints | `silver.company`, `geocode_cache` | the table rejects invalid states (unknown status, open versions not current, malformed NACE) even if code regresses |
| Liquid clustering | bronze (`_snapshot_date`), silver (`country_code, nace_division`) | file skipping for the per-country MERGE and the map queries, with no partition tuning |
| `delta.dataSkippingStatsColumns` | bronze, silver | stats only on filtered columns (Delta indexes the first 32 columns by default, and clustering keys must have stats) |
| Deletion vectors | all | cheap MERGE updates (no full file rewrites) |
| Auto-optimize / auto-compaction | all | no small-files problem, no scheduled OPTIMIZE job. On Unity Catalog managed tables, predictive optimisation also runs VACUUM/OPTIMIZE |
| `replaceWhere` | bronze, quarantine, stage | idempotent re-runs of a snapshot |
| Column comments | silver | the canonical model documents itself in Unity Catalog |

## Recovering

* **Re-run one country:** `databricks bundle run registry_lakehouse` with only that country's task
  repaired ("Repair run" in the UI). Bronze/silver are idempotent.
* **A bad snapshot was merged:** `RESTORE TABLE <catalog>.silver.company TO VERSION AS OF <n>`, fix,
  re-run. `DESCRIBE HISTORY` shows which run (`_run_id`) wrote which version.
* **Inspect rejects:** `SELECT failures, record FROM <catalog>.silver.quarantine WHERE country_code = 'FR'`.
