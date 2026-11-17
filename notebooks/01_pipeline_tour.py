# Databricks notebook source
# MAGIC %md
# MAGIC # EU company registry lakehouse: a tour of the data
# MAGIC
# MAGIC Six national company registers (FR, GB, IE, EE, ES, NO) flow through a medallion pipeline into
# MAGIC map-ready tables. This notebook only **reads**: run it from top to bottom to see what each layer
# MAGIC holds. Production data is written only by the scheduled job.
# MAGIC
# MAGIC | Where | What |
# MAGIC |---|---|
# MAGIC | Jobs & Pipelines → `registry-lakehouse-prod` → *Tasks* | the pipeline as a task graph |
# MAGIC | the same job → *Runs* | every run, with per-task timings, logs and skipped stages |
# MAGIC | `src/registry_lakehouse/` in this repository | the PySpark code the job runs (packaged as a wheel) |
# MAGIC | `02_inside_a_stage` | the code of one stage, step by step, on real data |
# MAGIC
# MAGIC ```
# MAGIC detect_changes ─► has_changes ─► bronze (per country) ─► silver (per country) ─┐
# MAGIC                               └► reference (gazetteer) ─────────────────────────┴► geo ─► gold ─► publish ─► cost_report
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog", "registry", "Catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Every table the pipeline owns
# MAGIC Table comments come from the code, so this list doubles as a data dictionary.

# COMMAND ----------

tables = spark.sql(f"""
    SELECT table_schema AS layer, table_name, table_type, comment
    FROM {catalog}.information_schema.tables
    WHERE table_schema IN ('bronze', 'silver', 'gold', 'ops')
    ORDER BY array_position(array('bronze', 'silver', 'gold', 'ops'), table_schema), table_name
""")
display(tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: raw registers, one table per country
# MAGIC All columns are kept as strings exactly as published, plus lineage columns
# MAGIC (`_source_file`, `_snapshot_date`, `_ingested_at`, `_run_id`). Every monthly snapshot is kept.

# COMMAND ----------

bronze_tables = [r.table_name for r in tables.where("layer = 'bronze' AND table_name LIKE '%_register'").collect()]
if bronze_tables:
    display(spark.sql(" UNION ALL ".join(
        f"SELECT '{t}' AS table_name, _snapshot_date, count(*) AS row_count, "
        f"count(_corrupt_record) AS malformed_lines, max(_ingested_at) AS ingested_at "
        f"FROM {catalog}.bronze.{t} GROUP BY _snapshot_date"
        for t in bronze_tables
    ) + " ORDER BY table_name, _snapshot_date DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: one canonical model for all six countries, with history (SCD2)
# MAGIC Each company has one current row and one closed row per past change (`valid_from` / `valid_to`).

# COMMAND ----------

display(spark.sql(f"""
    SELECT country_code, source_register, grain,
           count_if(is_current)                   AS companies,
           count_if(is_current AND is_active)     AS active_companies,
           count_if(NOT is_current)               AS past_versions,
           max(snapshot_date)                     AS latest_snapshot
    FROM {catalog}.silver.company
    GROUP BY ALL ORDER BY companies DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC Every pipeline write is a Delta commit. The history shows the monthly SCD2 `MERGE`s and what they changed.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {catalog}.silver.company LIMIT 20")
        .select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality
# MAGIC Rows that break an `ERROR` rule go to `silver.quarantine` instead of silver. Rule outcomes for
# MAGIC every snapshot are kept in `ops.dq_results`, and a breached threshold stops the run.

# COMMAND ----------

display(spark.sql(f"""
    SELECT country_code, snapshot_date, rule, count(*) AS rows_quarantined
    FROM {catalog}.silver.quarantine LATERAL VIEW explode(failures) AS rule
    GROUP BY ALL ORDER BY snapshot_date DESC, rows_quarantined DESC
"""))

# COMMAND ----------

display(spark.table(f"{catalog}.ops.dq_results"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: what the website shows

# COMMAND ----------

display(spark.sql(f"""
    SELECT country_code, nace_section, section_label, companies, active_companies
    FROM {catalog}.gold.agg_country_industry
    ORDER BY country_code, active_companies DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT * FROM {catalog}.gold.fishing_by_city
    ORDER BY fishing_companies DESC LIMIT 25
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operations: why a stage ran or was skipped, and what a run cost
# MAGIC `ops.stage_state` holds a fingerprint of each stage's inputs. When the inputs are unchanged, the
# MAGIC stage skips itself, which is why most monthly runs only pay for the registers that changed.

# COMMAND ----------

display(spark.table(f"{catalog}.ops.stage_state"))

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.ops.cost_by_run"):
    display(spark.table(f"{catalog}.ops.cost_by_run"))
else:
    print("No cost report yet: the cost_report task needs read access to system.billing (setup.sql step 5).")
