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

# # 10 · Bronze Ingestion (idempotent)
# Pulls raw data from all free sources and lands it in OneLake two ways:
# 1. **Raw files** under `Files/bronze/<source>/<ingestion_date>/` (immutable audit copy).
# 2. **Bronze Delta tables** via **MERGE** on each source's natural key — re-running never duplicates.
#
# Secrets are read from Key Vault / notebook params, NOT hard-coded.
#
# > **Setup:** attach the `mme` package via a Fabric *Environment* (custom library) or copy
# > `src/mme` into this notebook's built-in **Resources** and `sys.path.append` it.

# PARAMETERS CELL ********************

# Parameters (overridable by the Data Pipeline). Leave blank to pull from Key Vault.
fred_api_key = ""
sec_user_agent = ""
start_date = "2005-01-01"
ingestion_date = ""  # empty -> today

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os, sys, json, datetime as dt
# Resolve secrets (Key Vault preferred; fall back to the passed parameters).
try:
    kv = "https://<your-kv>.vault.azure.net/"
    fred_api_key = fred_api_key or notebookutils.credentials.getSecret(kv, "FRED-API-KEY")
    sec_user_agent = sec_user_agent or notebookutils.credentials.getSecret(kv, "SEC-USER-AGENT")
except Exception as e:
    print("Key Vault not configured, using parameters/env:", e)

os.environ["FRED_API_KEY"] = fred_api_key or os.environ.get("FRED_API_KEY", "")
os.environ["SEC_USER_AGENT"] = sec_user_agent or os.environ.get("SEC_USER_AGENT", "")
ingestion_date = ingestion_date or dt.date.today().isoformat()

# Make the reusable library importable (adjust path to where you uploaded src/).
sys.path.append("/lakehouse/default/Files/lib")        # if you copied src/mme here
from mme.ingestion import load_sources, FredClient, WorldBankClient, ImfClient, MarketClient, EdgarClient
from mme.common import add_audit_columns, merge_upsert

cfg = load_sources()
BRONZE_FILES = "Files/bronze"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def land_raw(pdf, source):
    """Write an immutable raw copy + return a Spark DF with audit columns."""
    path = f"{BRONZE_FILES}/{source}/ingestion_date={ingestion_date}"
    if pdf.empty:
        print(f"[{source}] 0 rows (source disabled or no data) — recording empty ingestion.")
        return None
    sdf = add_audit_columns(spark.createDataFrame(pdf), source, ingestion_date)
    sdf.write.format("parquet").mode("overwrite").save(path)   # raw audit copy
    return sdf

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- FRED (macro, with release dates) ----
fred = FredClient()
fred_pdf = fred.get_many(list(cfg["fred"]["series"].keys()), start=start_date)
sdf = land_raw(fred_pdf, "fred")
if sdf is not None:
    merge_upsert(spark, sdf, "bronze_fred", keys=["series_id", "obs_date"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- World Bank + IMF (sovereign) ----
wb = WorldBankClient()
wb_pdf = wb.get_many(list(cfg["world_bank"]["indicators"].keys()), cfg["world_bank"]["countries"])
sdf = land_raw(wb_pdf, "world_bank")
if sdf is not None:
    merge_upsert(spark, sdf, "bronze_world_bank", keys=["country_iso3", "indicator", "year"])

imf = ImfClient()
imf_pdf = imf.get_many(list(cfg["imf"]["indicators"].keys()), cfg["world_bank"]["countries"])
sdf = land_raw(imf_pdf, "imf")
if sdf is not None:
    merge_upsert(spark, sdf, "bronze_imf", keys=["country_iso3", "indicator", "year"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Market OHLCV (yfinance -> Stooq fallback) ----
m = cfg["market"]
all_tickers = sum(m["tickers"].values(), [])
mkt = MarketClient()
mkt_pdf = mkt.get_prices(all_tickers, start=m["start"])
sdf = land_raw(mkt_pdf, "market")
if sdf is not None:
    merge_upsert(spark, sdf, "bronze_market", keys=["ticker", "date"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- SEC EDGAR fundamentals (filing-dated) ----
ed = EdgarClient()
ed_pdf = ed.get_many(cfg["sec_edgar"]["tickers"], cfg["sec_edgar"]["concepts"])
sdf = land_raw(ed_pdf, "sec_edgar")
if sdf is not None:
    merge_upsert(spark, sdf, "bronze_sec_edgar", keys=["ticker", "concept", "end", "filed"])

print("Bronze ingestion complete for", ingestion_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
