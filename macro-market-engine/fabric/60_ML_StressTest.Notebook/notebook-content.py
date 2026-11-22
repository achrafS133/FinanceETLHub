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

# # 60 · Stress-Test Simulator (VAR + Monte Carlo)
# Precomputes shock scenarios so Power BI's What-If page is instant. For each scenario we:
# 1. Estimate the **regime-conditioned covariance** of asset returns (and a VAR for propagation).
# 2. Run a **Monte Carlo** of multi-day paths drawn from that covariance plus the scenario's
#    mean shock, for a representative set of assets.
# 3. Store per-asset expected drawdown, 1-day/10-day **VaR (95/99%)**, and recovery time in
#    `Fact_Stress_Scenario`. Tracked in MLflow.

# CELL ********************

import numpy as np, pandas as pd, mlflow

N_PATHS = 10000
HORIZON = 63          # ~3 trading months
SEED = 7
rng = np.random.default_rng(SEED)

# Named historical scenarios -> (regime to condition on, extra daily mean-return shock per asset-class).
SCENARIOS = {
    "2022 Rate Shock":     {"regime": "Stagflation",              "equity": -0.0010, "bond": -0.0008, "gold": 0.0003},
    "2008 Credit Crunch":  {"regime": "Contraction / Recession",  "equity": -0.0030, "bond":  0.0010, "gold": 0.0005},
    "1970s Stagflation":   {"regime": "Stagflation",              "equity": -0.0012, "bond": -0.0012, "gold": 0.0015},
    "Soft Landing":        {"regime": "Goldilocks / Expansion",   "equity":  0.0005, "bond":  0.0003, "gold": 0.0000},
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rets = spark.table("Fact_Market_Daily").select("DateKey", "Ticker", "log_ret_w").toPandas()
rets["date"] = pd.to_datetime(rets["DateKey"].astype(str), format="%Y%m%d")
regime = spark.table("Fact_Regime").select("DateKey", "RegimeName").toPandas()

wide = rets.pivot_table(index="DateKey", columns="Ticker", values="log_ret_w").merge(
    regime, on="DateKey", how="left"
)

asset_class = {t.Ticker: t.AssetClass for t in spark.table("Dim_Asset").select("Ticker", "AssetClass").collect()}

def class_shock(ticker, sc):
    ac = asset_class.get(ticker, "Equity")
    if ac == "Fixed Income": return sc["bond"]
    if ac == "Commodity":    return sc["gold"]
    return sc["equity"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mlflow.set_experiment("stress-test-mc")
rows = []

with mlflow.start_run(run_name="precompute_scenarios"):
    mlflow.log_params({"n_paths": N_PATHS, "horizon_days": HORIZON})

    for name, sc in SCENARIOS.items():
        sub = wide[wide["RegimeName"] == sc["regime"]].drop(columns=["DateKey", "RegimeName"]).dropna(axis=1, how="all")
        sub = sub.dropna(axis=0, how="any")
        tickers = list(sub.columns)
        if len(sub) < 60 or not tickers:
            continue

        cov = np.cov(sub.values, rowvar=False)
        mean = sub.mean().values + np.array([class_shock(t, sc) for t in tickers])

        # Monte Carlo: simulate HORIZON-day cumulative log-return paths.
        sims = rng.multivariate_normal(mean, cov, size=(N_PATHS, HORIZON))  # (paths, days, assets)
        cum = sims.cumsum(axis=1)                                           # cumulative log return
        price = np.exp(cum)                                                 # price path (start=1)

        for j, t in enumerate(tickers):
            p = price[:, :, j]
            terminal = p[:, -1] - 1.0
            running_max = np.maximum.accumulate(p, axis=1)
            drawdown = (p / running_max - 1.0).min(axis=1)          # worst drawdown per path
            # recovery time: days until path first climbs back above its start (1.0); else HORIZON
            recovered = (p >= 1.0)
            # first index (after day 0) where recovered; if never -> HORIZON
            rec_days = np.where(recovered.any(axis=1), recovered.argmax(axis=1), HORIZON)

            rows.append({
                "Scenario": name, "Ticker": t, "Regime": sc["regime"],
                "ExpectedReturn": float(terminal.mean()),
                "ExpectedDrawdown": float(drawdown.mean()),
                "VaR95_1d": float(-np.percentile(sims[:, 0, j], 5)),
                "VaR99_1d": float(-np.percentile(sims[:, 0, j], 1)),
                "VaR95_10d": float(-np.percentile(cum[:, 9, j], 5)),
                "RecoveryDays": float(np.median(rec_days)),
            })

stress_df = pd.DataFrame(rows)
print("Scenarios computed:", stress_df["Scenario"].nunique(), "| asset-rows:", len(stress_df))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not stress_df.empty:
    (spark.createDataFrame(stress_df)
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable("Fact_Stress_Scenario"))
    spark.sql("OPTIMIZE Fact_Stress_Scenario ZORDER BY (Scenario)")
    display(spark.table("Fact_Stress_Scenario").orderBy("Scenario", "Ticker"))
else:
    print("No scenarios produced — ensure Fact_Regime and Fact_Market_Daily are populated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
