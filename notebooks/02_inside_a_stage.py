# Databricks notebook source
# MAGIC %md
# MAGIC # Inside a stage: bronze → silver → gold, step by step
# MAGIC
# MAGIC The production job runs the package in `src/registry_lakehouse/` as a wheel. This notebook imports
# MAGIC the **same source code** from this repository and calls its transformation functions one by one
# MAGIC on a sample of real bronze rows, showing the code next to each result.
# MAGIC
# MAGIC Nothing is written: every step returns a DataFrame that is only displayed. To rerun the real
# MAGIC pipeline, use the job (*Run now*, or *Repair run* on one task).

# COMMAND ----------

dbutils.widgets.text("catalog", "registry", "Catalog")
dbutils.widgets.dropdown("country", "FR", ["FR", "GB", "IE", "EE", "ES", "NO"], "Country")
dbutils.widgets.text("sample_rows", "1000", "Sample rows")
dbutils.widgets.text("secret_scope", "registry-lakehouse", "Secret scope")

catalog = dbutils.widgets.get("catalog")
country = dbutils.widgets.get("country")
sample_rows = int(dbutils.widgets.get("sample_rows"))

# COMMAND ----------

# Import the package from the repository source (../src), not from an installed wheel.
import inspect
import os
import sys
from pathlib import Path

src = str(Path.cwd().parent / "src")
if src not in sys.path:
    sys.path.insert(0, src)

# Spain pseudonymises personal identifiers with a secret key; read it if you have access.
try:
    os.environ["REGISTRY_PSEUDONYM_KEY"] = dbutils.secrets.get(dbutils.widgets.get("secret_scope"), "pseudonym-key")
except Exception:
    if country == "ES":
        raise RuntimeError("ES needs READ on the secret scope (pseudonym-key) to reproduce silver ids") from None

from pyspark.sql import functions as F

from registry_lakehouse.bronze.load import bronze_table_name
from registry_lakehouse.config import Settings
from registry_lakehouse.gold import build as gold
from registry_lakehouse.quality import engine
from registry_lakehouse.quality.rules import rules_for
from registry_lakehouse.silver import build as silver
from registry_lakehouse.silver.sources import MAPPERS

settings = Settings(landing_root="", catalog=catalog)


def show_code(fn) -> None:
    print(inspect.getsource(fn))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze: the register as published
# MAGIC The `bronze` task reads the landing file with an all-string schema, checks it against the source
# MAGIC contract (`contracts/sources.yml`) and adds lineage columns. Here: a sample of the latest snapshot.

# COMMAND ----------

bronze_all = spark.table(bronze_table_name(settings, country)).where(F.col("_corrupt_record").isNull())
snapshot = bronze_all.agg(F.max("_snapshot_date")).first()[0]
bronze_sample = silver._bronze_for(bronze_all, country, snapshot).limit(sample_rows)
print(f"{bronze_table_name(settings, country)}, snapshot {snapshot}")
display(bronze_sample)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver, step 1: map the national format to the canonical model
# MAGIC Each country has a mapper (`silver/sources/<country>.py`). `finalise` then adds the keys, NACE
# MAGIC classification and the `hash_diff` used to detect changes between snapshots.

# COMMAND ----------

show_code(MAPPERS[country].mapper)

# COMMAND ----------

canonical = silver.canonical_snapshot(bronze_sample, country, snapshot)
display(canonical)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver, step 2: data-quality rules
# MAGIC Every row is annotated with the rules it fails. `ERROR` rules send the row to quarantine; a failure
# MAGIC rate above a rule's threshold stops the whole run.

# COMMAND ----------

rules = rules_for(country)
display(spark.createDataFrame(
    [(r.name, r.severity, r.max_failure_rate, r.description) for r in rules],
    "rule STRING, severity STRING, max_failure_rate DOUBLE, description STRING",
))

# COMMAND ----------

annotated = engine.annotate(canonical, rules)
total, failures = engine.measure(annotated, rules)
print(f"{total} rows in the sample")
display(spark.createDataFrame(sorted(failures.items(), key=lambda kv: -kv[1]), "rule STRING, failing_rows LONG"))

# COMMAND ----------

display(annotated.where(F.size(engine.FAILURES_COL) > 0)
        .select("business_id", "company_name", "status", engine.FAILURES_COL, engine.QUARANTINE_COL))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver, step 3: deduplicate, then compare with the stored table
# MAGIC The real task then MERGEs these rows into `silver.company` as SCD2 versions (`silver/scd2.py`).

# COMMAND ----------

show_code(silver.deduplicate)

# COMMAND ----------

clean = silver.deduplicate(annotated.where(~F.col(engine.QUARANTINE_COL)))
stored = spark.table(settings.table("silver", "company")).where(f"is_current AND country_code = '{country}'")
display(clean.join(stored.select("company_key", F.col("hash_diff").alias("stored_hash_diff"), "valid_from"),
                   "company_key", "left")
        .select("company_key", "company_name", "status", "nace_code",
                (F.col("hash_diff") == F.col("stored_hash_diff")).alias("matches_stored_version"), "valid_from"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold: map-ready aggregates
# MAGIC Gold is recomputed from the current silver rows. Each table is a pure DataFrame function, so here
# MAGIC it runs on the stored company dimension and its result equals the stored gold table.

# COMMAND ----------

show_code(gold.agg_country_industry)

# COMMAND ----------

dim = spark.table(settings.table("gold", "dim_company")).where(F.col("country_code") == country)
display(gold.agg_country_industry(dim).orderBy(F.desc("active_companies")))
