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

# # 40 · Macro Regime Detection (Gaussian HMM)
# Trains an unsupervised **Hidden Markov Model** on three macro axes — **Growth**
# (Industrial Production YoY), **Inflation** (CPI YoY), **Financial Conditions**
# (10Y-2Y spread + HY credit spread) — and labels every month into a latent regime
# (Goldilocks / Reflation / Stagflation / Contraction). Tracked in **MLflow**; output
# written to `Fact_Regime` for Power BI.

# CELL ********************

import numpy as np, pandas as pd, mlflow
from hmmlearn.hmm import GaussianHMM
from sklearn.preprocessing import StandardScaler

N_REGIMES = 4
RANDOM_STATE = 42

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build a monthly, point-in-time macro feature frame from the Gold facts.
macro = spark.table("Fact_Macro_Monthly").toPandas()
macro["date"] = pd.to_datetime(macro["DateKey"].astype(str), format="%Y%m%d")

def series(sid, col="yoy_pct"):
    s = macro[macro["SeriesId"] == sid].set_index("date")[col].sort_index()
    return s.resample("MS").last()

feat = pd.DataFrame({
    "growth":    series("INDPRO", "yoy_pct"),
    "inflation": series("CPIAUCSL", "yoy_pct"),
    "curve":     series("T10Y2Y", "value"),
    "credit":    series("BAMLH0A0HYM2", "value"),
}).dropna()

print("Feature window:", feat.index.min(), "→", feat.index.max(), feat.shape)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mlflow.set_experiment("macro-regime-hmm")
with mlflow.start_run(run_name=f"hmm_{N_REGIMES}state") as run:
    X = StandardScaler().fit_transform(feat.values)
    model = GaussianHMM(n_components=N_REGIMES, covariance_type="full",
                        n_iter=500, random_state=RANDOM_STATE)
    model.fit(X)
    states = model.predict(X)
    logprob = model.score(X)

    mlflow.log_params({"n_regimes": N_REGIMES, "covariance_type": "full",
                       "features": ",".join(feat.columns)})
    mlflow.log_metric("log_likelihood", float(logprob))
    mlflow.log_metric("aic", float(-2 * logprob + 2 * model.n_components ** 2))

    # Name regimes from each state's mean growth/inflation profile (interpretable labels).
    prof = pd.DataFrame(model.means_, columns=feat.columns)
    def name_state(r):
        hi_inf = r["inflation"] > prof["inflation"].median()
        hi_grw = r["growth"] > prof["growth"].median()
        if hi_grw and not hi_inf:  return "Goldilocks / Expansion"
        if hi_grw and hi_inf:      return "Reflation"
        if not hi_grw and hi_inf:  return "Stagflation"
        return "Contraction / Recession"
    labels = {i: name_state(prof.loc[i]) for i in range(N_REGIMES)}
    mlflow.log_dict({str(k): v for k, v in labels.items()}, "regime_labels.json")

    out = feat.copy()
    out["RegimeId"] = states
    out["RegimeName"] = out["RegimeId"].map(labels)
    out = out.reset_index().rename(columns={"index": "date"})
    out["DateKey"] = out["date"].dt.strftime("%Y%m%d").astype(int)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Expand monthly regime labels to a daily lookup so any daily fact can join to it,
# then persist as Fact_Regime.
daily = spark.table("Dim_Date").filter("IsTradingDay = true").select("DateKey", "Date").toPandas()
daily["date"] = pd.to_datetime(daily["DateKey"].astype(str), format="%Y%m%d")
reg = out[["date", "RegimeId", "RegimeName", "growth", "inflation", "curve", "credit"]].sort_values("date")
daily = pd.merge_asof(daily.sort_values("date"), reg, on="date", direction="backward").dropna(subset=["RegimeName"])

fact_regime = spark.createDataFrame(
    daily[["DateKey", "RegimeId", "RegimeName", "growth", "inflation", "curve", "credit"]]
)
(fact_regime.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("Fact_Regime"))
spark.sql("OPTIMIZE Fact_Regime ZORDER BY (DateKey)")
display(spark.table("Fact_Regime").groupBy("RegimeName").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
