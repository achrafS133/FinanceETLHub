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

# # 20 · Silver — cleaning, alignment & as-of joins
# The senior-level layer. Here we:
# * **Clean market data**: adjusted close only, flag + winsorize bad-tick returns.
# * **Stationarity transforms**: log returns, 30/90/252-day annualized volatility.
# * **Macro frequency sync**: compute YoY/MoM, align each release to the *next trading day*.
# * **As-of join** (point-in-time): each trading day sees only macro values already *released*.

# CELL ********************

from pyspark.sql import functions as F, Window as W

TRADING = spark.table("Dim_Date").filter("IsTradingDay = true").select("Date", "DateKey", "NextTradingDay")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2.1 Market → `silver_market_daily`

# CELL ********************

mkt = spark.table("bronze_market")

w_tkr = W.partitionBy("ticker").orderBy("date")
mkt = (
    mkt
    .withColumn("date", F.to_date("date"))
    .withColumn("prev_adj", F.lag("adj_close").over(w_tkr))
    .withColumn("log_ret", F.log(F.col("adj_close") / F.col("prev_adj")))
    .withColumn("simple_ret", F.col("adj_close") / F.col("prev_adj") - 1.0)
)

# Flag bad ticks: |z-score| of daily log return > 8 within each ticker.
stats = mkt.groupBy("ticker").agg(F.avg("log_ret").alias("mu"), F.stddev_pop("log_ret").alias("sd"))
mkt = (
    mkt.join(stats, "ticker")
    .withColumn("ret_z", (F.col("log_ret") - F.col("mu")) / F.col("sd"))
    .withColumn("is_outlier", F.abs("ret_z") > F.lit(8.0))
)

# Winsorize log returns to the [1%, 99%] per-ticker quantiles.
bounds = (
    mkt.groupBy("ticker")
    .agg(F.expr("percentile_approx(log_ret, 0.01)").alias("p01"),
         F.expr("percentile_approx(log_ret, 0.99)").alias("p99"))
)
mkt = (
    mkt.join(bounds, "ticker")
    .withColumn("log_ret_w", F.least(F.greatest("log_ret", "p01"), "p99"))
)

# Rolling annualized volatility (trading-day windows).
def roll_vol(col, n):
    win = w_tkr.rowsBetween(-(n - 1), 0)
    return F.stddev_pop(col).over(win) * F.sqrt(F.lit(252.0))

silver_market = (
    mkt
    .withColumn("vol_30", roll_vol("log_ret_w", 30))
    .withColumn("vol_90", roll_vol("log_ret_w", 90))
    .withColumn("vol_252", roll_vol("log_ret_w", 252))
    .join(TRADING.select("date", "DateKey"), "date", "inner")  # keep only trading days
    .select("DateKey", "date", "ticker", "open", "high", "low", "close", "adj_close",
            "volume", "log_ret", "log_ret_w", "simple_ret", "vol_30", "vol_90", "vol_252",
            "is_outlier")
)

(silver_market.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_market_daily"))
print("silver_market_daily:", spark.table("silver_market_daily").count(), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2.2 Macro → `silver_macro` (release-aligned, YoY/MoM)
# `release_date` is the day the value became public. We align it to the next trading day
# so it can be as-of joined to market data without ever peeking into the future.

# CELL ********************

macro = spark.table("bronze_fred").withColumn("obs_date", F.to_date("obs_date")) \
                                  .withColumn("release_date", F.to_date("release_date"))

w_ser = W.partitionBy("series_id").orderBy("obs_date")
macro = (
    macro
    .withColumn("value_lag12", F.lag("value", 12).over(w_ser))
    .withColumn("value_lag1",  F.lag("value", 1).over(w_ser))
    .withColumn("yoy_pct", (F.col("value") / F.col("value_lag12") - 1) * 100)
    .withColumn("mom_pct", (F.col("value") / F.col("value_lag1") - 1) * 100)
)

# Align the publication date forward to the next trading day.
macro = (
    macro.join(TRADING.select(F.col("Date").alias("release_date"), F.col("NextTradingDay")),
               "release_date", "left")
    .withColumn("effective_date", F.coalesce("NextTradingDay", "release_date"))
    .withColumn("EffectiveDateKey", F.date_format("effective_date", "yyyyMMdd").cast("int"))
)

(macro.select("series_id", "obs_date", "release_date", "effective_date", "EffectiveDateKey",
              "value", "yoy_pct", "mom_pct")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_macro"))
print("silver_macro:", spark.table("silver_macro").count(), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2.3 Point-in-time macro panel → `silver_macro_pit`
# For each trading day and each series, carry forward the latest *effective* (released)
# value. This is the as-of join implemented in Spark via a forward-fill on effective_date.

# CELL ********************

series_ids = [r["series_id"] for r in spark.table("silver_macro").select("series_id").distinct().collect()]

# Cross trading-day calendar with each series, then forward-fill the last released value.
cal = TRADING.select("date", "DateKey")
grid = cal.crossJoin(spark.createDataFrame([(s,) for s in series_ids], ["series_id"]))

m = spark.table("silver_macro").select(
    F.col("effective_date").alias("date"), "series_id",
    F.col("value").alias("macro_value"), "yoy_pct", "mom_pct"
)

joined = grid.join(m, ["date", "series_id"], "left")
w_ff = W.partitionBy("series_id").orderBy("date").rowsBetween(W.unboundedPreceding, 0)
pit = (
    joined
    .withColumn("macro_value", F.last("macro_value", ignorenulls=True).over(w_ff))
    .withColumn("yoy_pct",     F.last("yoy_pct", ignorenulls=True).over(w_ff))
    .withColumn("mom_pct",     F.last("mom_pct", ignorenulls=True).over(w_ff))
    .filter(F.col("macro_value").isNotNull())
)

(pit.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("silver_macro_pit"))
print("silver_macro_pit:", spark.table("silver_macro_pit").count(), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2.4 Fundamentals → `silver_fundamentals` (filing-date aligned)

# CELL ********************

if spark.catalog.tableExists("bronze_sec_edgar"):
    fun = (
        spark.table("bronze_sec_edgar")
        .withColumn("end", F.to_date("end"))
        .withColumn("filed", F.to_date("filed"))
        .join(TRADING.select(F.col("Date").alias("filed"), F.col("NextTradingDay")), "filed", "left")
        .withColumn("effective_date", F.coalesce("NextTradingDay", "filed"))
        .withColumn("EffectiveDateKey", F.date_format("effective_date", "yyyyMMdd").cast("int"))
        .select("ticker", "cik", "concept", "end", "filed", "effective_date",
                "EffectiveDateKey", "form", "value")
    )
    (fun.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable("silver_fundamentals"))
    print("silver_fundamentals:", spark.table("silver_fundamentals").count(), "rows")
else:
    print("bronze_sec_edgar not present (SEC_USER_AGENT unset) — skipping fundamentals.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
