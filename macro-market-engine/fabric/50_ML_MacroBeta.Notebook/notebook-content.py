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

# # 50 · Macro Factor Sensitivity & Beta Attribution
# Regresses each asset's **excess return** on **macro surprises** (daily changes in rates,
# inflation proxy, USD, oil, credit) using **ElasticNet**. Coefficients form the dynamic
# "Macro Beta" matrix; **SHAP** values quantify each factor's contribution. The betas answer:
# *which sectors are most exposed to a +100 bps rate move or a USD spike?* Tracked in MLflow;
# output written to `Fact_Macro_Beta`.

# CELL ********************

import numpy as np, pandas as pd, mlflow
from sklearn.linear_model import ElasticNetCV
from sklearn.preprocessing import StandardScaler

# Macro "surprise" = daily first-difference of the point-in-time level.
MACRO_FACTORS = ["DGS10", "T10Y2Y", "CPIAUCSL", "DTWEXBGS", "BAMLH0A0HYM2"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

feat = spark.table("Feature_Market_Macro").toPandas()
feat["date"] = pd.to_datetime(feat["DateKey"].astype(str), format="%Y%m%d")

# Build daily macro surprises (differences) from the wide macro columns.
macro_cols = [c for c in MACRO_FACTORS if c in feat.columns]
surprises = (
    feat[["date"] + macro_cols].drop_duplicates("date").sort_values("date").set_index("date")
    .astype(float).diff().add_suffix("_chg")
)
factor_cols = list(surprises.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mlflow.set_experiment("macro-beta-attribution")
results = []

for ticker, g in feat.groupby("Ticker"):
    g = g[["date", "excess_ret"]].dropna().sort_values("date").set_index("date")
    df = g.join(surprises, how="inner").dropna()
    if len(df) < 250:          # need ~1y of overlap
        continue
    X = df[factor_cols].values
    y = df["excess_ret"].values
    Xs = StandardScaler().fit_transform(X)

    with mlflow.start_run(run_name=f"beta_{ticker}", nested=False):
        model = ElasticNetCV(l1_ratio=[0.1, 0.5, 0.9], cv=5, random_state=42, max_iter=5000)
        model.fit(Xs, y)
        r2 = model.score(Xs, y)
        mlflow.log_params({"ticker": ticker, "alpha": float(model.alpha_), "l1_ratio": float(model.l1_ratio_)})
        mlflow.log_metric("r2", float(r2))

        betas = dict(zip(factor_cols, model.coef_))
        # SHAP for a linear model = coef * (x - mean); summarize mean |SHAP| per factor.
        try:
            import shap
            expl = shap.LinearExplainer(model, Xs)
            shap_vals = np.abs(expl.shap_values(Xs)).mean(axis=0)
            shap_map = dict(zip(factor_cols, shap_vals))
        except Exception:
            shap_map = {c: abs(b) for c, b in betas.items()}

        for f in factor_cols:
            results.append({
                "Ticker": ticker, "Factor": f, "Beta": float(betas[f]),
                "MeanAbsShap": float(shap_map[f]), "R2": float(r2),
            })

beta_df = pd.DataFrame(results)
print("Estimated betas for", beta_df["Ticker"].nunique(), "assets")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not beta_df.empty:
    (spark.createDataFrame(beta_df)
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable("Fact_Macro_Beta"))
    display(spark.table("Fact_Macro_Beta").orderBy("Ticker", "Factor"))
else:
    print("No assets had enough overlap — check that Silver/Gold ran with market + macro data.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
