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

# # 30 · Gold — Star Schema + Feature Store
# Builds dimensions, fact tables, and the wide ML feature table, then applies **V-Order**
# + `OPTIMIZE ZORDER BY (DateKey)` so Direct Lake queries stay fast.

# CELL ********************

from pyspark.sql import functions as F, Window as W

# Ensure V-Order is on for all writes in this session (optimizes Parquet for Direct Lake).
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3.1 Dimensions
# `Dim_Date` already exists from notebook 00. Build `Dim_Asset`, `Dim_Country`, `Dim_Macro_Indicator`.

# CELL ********************

# Dim_Asset: ticker -> asset class / GICS sector / region (seed mapping; extend as needed).
asset_rows = [
    # ticker, name, asset_class, gics_sector, region
    ("SPY","S&P 500 ETF","Equity","Broad Market","US"),
    ("AGG","US Aggregate Bond ETF","Fixed Income","Aggregate","US"),
    ("TLT","20+Y Treasury ETF","Fixed Income","Government","US"),
    ("GLD","Gold ETF","Commodity","Precious Metals","Global"),
    ("XLK","Technology Select","Equity","Information Technology","US"),
    ("XLF","Financials Select","Equity","Financials","US"),
    ("XLE","Energy Select","Equity","Energy","US"),
    ("XLV","Health Care Select","Equity","Health Care","US"),
    ("XLY","Cons. Discretionary","Equity","Consumer Discretionary","US"),
    ("XLP","Cons. Staples","Equity","Consumer Staples","US"),
    ("XLI","Industrials Select","Equity","Industrials","US"),
    ("XLB","Materials Select","Equity","Materials","US"),
    ("XLU","Utilities Select","Equity","Utilities","US"),
    ("XLRE","Real Estate Select","Equity","Real Estate","US"),
    ("XLC","Comm. Services","Equity","Communication Services","US"),
    ("EWJ","Japan ETF","Equity","Country","Developed"),
    ("EWG","Germany ETF","Equity","Country","Developed"),
    ("EWZ","Brazil ETF","Equity","Country","Emerging"),
    ("EWU","UK ETF","Equity","Country","Developed"),
    ("EWC","Canada ETF","Equity","Country","Developed"),
    ("EWA","Australia ETF","Equity","Country","Developed"),
    ("EWY","South Korea ETF","Equity","Country","Emerging"),
    ("INDA","India ETF","Equity","Country","Emerging"),
    ("MCHI","China ETF","Equity","Country","Emerging"),
    ("UUP","US Dollar Bull ETF","Currency","USD","US"),
    ("USO","US Oil Fund","Commodity","Energy","Global"),
]
dim_asset = spark.createDataFrame(asset_rows, ["Ticker","AssetName","AssetClass","GICSSector","Region"])
dim_asset.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("Dim_Asset")

# Dim_Country: ISO3 -> region / developed vs emerging (seed).
country_rows = [
    ("USA","North America","Developed"),("CHN","Asia","Emerging"),("JPN","Asia","Developed"),
    ("DEU","Europe","Developed"),("GBR","Europe","Developed"),("FRA","Europe","Developed"),
    ("IND","Asia","Emerging"),("BRA","Latin America","Emerging"),("CAN","North America","Developed"),
    ("AUS","Oceania","Developed"),("KOR","Asia","Emerging"),("MEX","Latin America","Emerging"),
    ("ITA","Europe","Developed"),("ESP","Europe","Developed"),("RUS","Europe","Emerging"),
]
spark.createDataFrame(country_rows, ["CountryISO3","Region","MarketClass"]) \
    .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("Dim_Country")

# Dim_Macro_Indicator: series_id -> human label / category.
ind_rows = [
    ("FEDFUNDS","Fed Funds Rate","Policy Rate"),("CPIAUCSL","CPI","Inflation"),
    ("PCEPILFE","Core PCE","Inflation"),("T10Y2Y","10Y-2Y Spread","Financial Conditions"),
    ("DGS10","10Y Treasury","Rates"),("DGS2","2Y Treasury","Rates"),
    ("UNRATE","Unemployment Rate","Labor"),("INDPRO","Industrial Production","Growth"),
    ("BAMLH0A0HYM2","HY Credit Spread","Financial Conditions"),("DTWEXBGS","Broad USD Index","FX"),
]
spark.createDataFrame(ind_rows, ["SeriesId","IndicatorName","Category"]) \
    .write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("Dim_Macro_Indicator")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3.2 Fact tables

# CELL ********************

# Fact_Market_Daily
fact_market = spark.table("silver_market_daily").select(
    "DateKey","ticker","adj_close","log_ret_w","simple_ret","vol_30","vol_90","vol_252","volume","is_outlier"
).withColumnRenamed("ticker","Ticker")
fact_market.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("Fact_Market_Daily")

# Fact_Macro_Monthly (one row per series per reference month, with release/effective keys)
fact_macro = spark.table("silver_macro").select(
    F.date_format("obs_date","yyyyMMdd").cast("int").alias("DateKey"),
    "EffectiveDateKey", F.col("series_id").alias("SeriesId"), "value","yoy_pct","mom_pct"
)
fact_macro.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("Fact_Macro_Monthly")

# Fact_Company_Quarterly (if fundamentals exist)
if spark.catalog.tableExists("silver_fundamentals"):
    fact_co = spark.table("silver_fundamentals").select(
        F.date_format("end","yyyyMMdd").cast("int").alias("DateKey"),
        "EffectiveDateKey", F.col("ticker").alias("Ticker"), "concept","value","form"
    )
    fact_co.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("Fact_Company_Quarterly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3.3 ML feature table
# Wide, point-in-time-correct panel: each trading day × asset, with excess return vs the
# risk-free proxy and every macro series pivoted to columns (from `silver_macro_pit`).

# CELL ********************

# Risk-free daily proxy from Fed Funds (annual % -> daily).
rf = (
    spark.table("silver_macro_pit").filter("series_id = 'FEDFUNDS'")
    .select("DateKey", (F.col("macro_value") / 100.0 / 252.0).alias("rf_daily"))
)

macro_wide = (
    spark.table("silver_macro_pit")
    .groupBy("DateKey")
    .pivot("series_id")
    .agg(F.first("macro_value"))
)

features = (
    spark.table("silver_market_daily")
    .select("DateKey","ticker","log_ret_w","vol_30","vol_90")
    .join(rf, "DateKey", "left")
    .withColumn("excess_ret", F.col("log_ret_w") - F.coalesce(F.col("rf_daily"), F.lit(0.0)))
    .join(macro_wide, "DateKey", "left")
    .withColumnRenamed("ticker","Ticker")
)
features.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("Feature_Market_Macro")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3.4 Optimize for Direct Lake

# CELL ********************

for t in ["Fact_Market_Daily","Fact_Macro_Monthly","Feature_Market_Macro"]:
    spark.sql(f"OPTIMIZE {t} ZORDER BY (DateKey)")
if spark.catalog.tableExists("Fact_Company_Quarterly"):
    spark.sql("OPTIMIZE Fact_Company_Quarterly ZORDER BY (DateKey)")

print("Gold star schema built & optimized.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
