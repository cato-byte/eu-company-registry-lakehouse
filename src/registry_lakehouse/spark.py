"""SparkSession factory and Delta helpers shared by every layer.

On Databricks the active (serverless, Spark Connect) session is reused. Locally a Delta-enabled
session is built with ANSI mode on, matching serverless, so casts that would throw in
production also throw in the tests.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "registry-lakehouse", warehouse_dir: str | None = None) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    if on_databricks():
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    warehouse = os.path.abspath(warehouse_dir or os.environ.get("REGISTRY_LOCAL_WAREHOUSE", ".local/warehouse"))
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("REGISTRY_SPARK_MASTER", "local[4]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.snapshotPartitions", "2")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    _register_local_tables(spark, warehouse)
    return spark


def _register_local_tables(spark: SparkSession, warehouse: str) -> None:
    """Re-attach Delta tables left in the local warehouse by an earlier run.

    Locally the catalog is in-memory, so tables written by `registry-lakehouse bronze` would
    be invisible to a later `registry-lakehouse silver` process. Unity Catalog does this on
    Databricks. Here, every <schema>.db/<table>/_delta_log directory is registered again.
    """
    if not os.path.isdir(warehouse):
        return
    for schema_dir in sorted(os.listdir(warehouse)):
        if not schema_dir.endswith(".db"):
            continue
        schema = schema_dir[:-3]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS spark_catalog.{schema}")
        for table in sorted(os.listdir(os.path.join(warehouse, schema_dir))):
            location = os.path.join(warehouse, schema_dir, table)
            if os.path.isdir(os.path.join(location, "_delta_log")):
                name = f"spark_catalog.{schema}.{table}"
                spark.sql(f"CREATE TABLE IF NOT EXISTS {name} USING DELTA LOCATION '{location}'")


def ensure_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def table_exists(spark: SparkSession, full_name: str) -> bool:
    return bool(spark.catalog.tableExists(full_name))


# Table properties applied to every managed table. Deletion vectors make MERGE/DELETE cheap.
# File layout and maintenance (OPTIMIZE, VACUUM, compaction) are left to predictive optimization
# on Unity Catalog managed tables, so no per-table autoOptimize settings are pinned here. Change
# Data Feed is enabled only where a consumer reads it (silver.company, see silver/scd2.py).
DEFAULT_TBLPROPERTIES = {
    "delta.enableDeletionVectors": "true",
}


def create_table_if_missing(
    spark: SparkSession,
    full_name: str,
    ddl_columns: str,
    cluster_by: list[str] | None = None,
    comment: str | None = None,
    constraints: dict[str, str] | None = None,
    extra_properties: dict[str, str] | None = None,
) -> None:
    """Create a Delta table once, with liquid clustering, CHECK constraints and documentation.

    Safe when several tasks race to create the same table (the job runs countries in parallel):
    IF NOT EXISTS for the table, and constraints that already exist are left alone.
    """
    if table_exists(spark, full_name):
        return
    props = {**DEFAULT_TBLPROPERTIES, **(extra_properties or {})}
    props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
    cluster_sql = f"CLUSTER BY ({', '.join(cluster_by)})" if cluster_by else ""
    comment_sql = "COMMENT '{}'".format(comment.replace("'", "\\'")) if comment else ""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {full_name} ({ddl_columns}) USING DELTA {cluster_sql} {comment_sql} "
        f"TBLPROPERTIES ({props_sql})"
    )
    existing = {r.key for r in spark.sql(f"SHOW TBLPROPERTIES {full_name}").collect()}  # spark-lint: ok[SPK001] props
    for name, expr in (constraints or {}).items():
        if f"delta.constraints.{name.lower()}" in existing:
            continue
        try:
            spark.sql(f"ALTER TABLE {full_name} ADD CONSTRAINT {name} CHECK ({expr})")
        except Exception as exc:  # another task added it in the meantime
            if "already exists" not in str(exc).lower():
                raise
    log.info("created table %s", full_name)


def overwrite_partition(df: DataFrame, full_name: str, predicate: str) -> None:
    """Idempotent load: replace exactly the slice described by ``predicate`` (e.g. one snapshot)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(full_name)
    )
