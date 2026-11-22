"""Idempotent Delta write helpers (used inside Fabric notebooks).

Kept framework-light: these take a SparkSession so they import cleanly in a
pure-Python test environment without PySpark installed.
"""
from __future__ import annotations

import datetime as _dt
from typing import List


def add_audit_columns(sdf, source: str, ingestion_date: str | None = None):
    """Append lineage/audit columns to a Spark DataFrame."""
    from pyspark.sql import functions as F

    ingestion_date = ingestion_date or _dt.date.today().isoformat()
    return (
        sdf.withColumn("_source", F.lit(source))
        .withColumn("_ingestion_date", F.lit(ingestion_date).cast("date"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def merge_upsert(spark, sdf, target_table: str, keys: List[str]):
    """Idempotent MERGE: insert new rows, update changed ones, never duplicate.

    Creates the table on first run. ``keys`` is the natural/business key set that
    uniquely identifies a record (e.g. ['series_id','obs_date'] for FRED).
    """
    from delta.tables import DeltaTable

    if not spark.catalog.tableExists(target_table):
        sdf.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return

    target = DeltaTable.forName(spark, target_table)
    cond = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])
    (
        target.alias("t")
        .merge(sdf.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
