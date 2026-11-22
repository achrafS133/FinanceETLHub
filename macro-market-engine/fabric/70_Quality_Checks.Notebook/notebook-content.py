# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "MacroMarketLH",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 70 · Data quality gate
# Runs the eight governance checks documented in `docs/03_data-quality-governance.md`.
# Any failure **raises** so the Data Pipeline activity fails. Results append to
# `Gold_Audit_Quality_Checks`; row counts to `Gold_Audit_Run_Metrics` for drift checks.

# PARAMETERS CELL ********************

drift_tolerance = 0.15
outlier_max_rate = 0.01
ingestion_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime as dt
import sys

sys.path.append("/lakehouse/default/Files/lib")

from pyspark.sql import Row, functions as F

from mme.quality.checks import (
    CheckResult,
    spark_non_null,
    spark_no_lookahead,
    spark_outlier_rate,
    spark_positive_prices,
    spark_referential,
    spark_regime_labels,
    spark_row_count_drift,
    spark_unique,
)

ingestion_date = ingestion_date or dt.date.today().isoformat()
run_ts = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
results: list[CheckResult] = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)

# 1–3 Silver
if table_exists("silver_market_daily"):
    results.append(spark_non_null(spark, "silver_market_daily", ["DateKey", "ticker"], "non_null_market_silver"))
    results.append(spark_positive_prices(spark, "silver_market_daily", "positive_prices_silver"))
    results.append(
        spark_outlier_rate(spark, "silver_market_daily", "outlier_rate_silver", max_rate=outlier_max_rate)
    )

if table_exists("silver_macro"):
    results.append(spark_non_null(spark, "silver_macro", ["series_id", "release_date"], "non_null_macro_silver"))
    results.append(spark_no_lookahead(spark, "silver_macro", "no_lookahead_silver"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2, 8 Gold referential + uniqueness
if table_exists("Fact_Market_Daily"):
    results.append(spark_unique(spark, "Fact_Market_Daily", ["Ticker", "DateKey"], "unique_fact_market"))
    results.append(
        spark_referential(spark, "Fact_Market_Daily", "Dim_Asset", "Ticker", "Ticker", "referential_market_asset")
    )

if table_exists("Fact_Macro_Beta"):
    results.append(
        spark_referential(spark, "Fact_Macro_Beta", "Dim_Asset", "Ticker", "Ticker", "referential_beta_asset")
    )

if table_exists("Fact_Stress_Scenario"):
    results.append(
        spark_referential(
            spark, "Fact_Stress_Scenario", "Dim_Asset", "Ticker", "Ticker", "referential_stress_asset"
        )
    )

# 7 Regime labels
if table_exists("Fact_Regime"):
    results.append(spark_regime_labels(spark, "Fact_Regime", "regime_labels_valid"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6 Row-count drift (after we snapshot prior run from Gold_Audit_Run_Metrics)
METRICS_TABLE = "Gold_Audit_Run_Metrics"
DRIFT_TABLES = ["Fact_Market_Daily", "silver_market_daily", "silver_macro"]
for tbl in DRIFT_TABLES:
    if table_exists(tbl):
        results.append(
            spark_row_count_drift(spark, tbl, METRICS_TABLE, f"row_drift_{tbl}", tolerance=drift_tolerance)
        )

failures = [r for r in results if r.status == "fail"]
print(f"Checks: {len(results)} total, {len(failures)} failed, {sum(1 for r in results if r.status=='skip')} skipped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

audit_rows = [
    Row(
        check_name=r.check_name,
        status=r.status,
        metric_value=float(r.metric_value) if r.metric_value == r.metric_value else None,
        detail=r.detail,
        ingestion_date=ingestion_date,
        run_timestamp=run_ts,
    )
    for r in results
]
audit_df = spark.createDataFrame(audit_rows)
if spark.catalog.tableExists("Gold_Audit_Quality_Checks"):
    audit_df.write.format("delta").mode("append").saveAsTable("Gold_Audit_Quality_Checks")
else:
    audit_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        "Gold_Audit_Quality_Checks"
    )

# Snapshot row counts for next run's drift check
metric_rows = []
for tbl in DRIFT_TABLES:
    if table_exists(tbl):
        metric_rows.append(
            Row(table_name=tbl, row_count=spark.table(tbl).count(), run_timestamp=run_ts, ingestion_date=ingestion_date)
        )
if metric_rows:
    metrics_df = spark.createDataFrame(metric_rows)
    if spark.catalog.tableExists(METRICS_TABLE):
        metrics_df.write.format("delta").mode("append").saveAsTable(METRICS_TABLE)
    else:
        metrics_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(METRICS_TABLE)

display(spark.table("Gold_Audit_Quality_Checks").orderBy(F.col("run_timestamp").desc()).limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if failures:
    msg = "; ".join(f"{f.check_name}: {f.detail}" for f in failures)
    raise RuntimeError(f"Quality gate failed — {msg}")

print("Quality gate passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }