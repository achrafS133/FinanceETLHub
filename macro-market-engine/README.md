# Global Macro-to-Market Impact Engine

An end-to-end **Microsoft Fabric Lakehouse** platform that ingests public macroeconomic
and market data, models how macro shocks (inflation, rate hikes, GDP shifts) propagate
across equities, bonds, and sovereign economies, and serves an interactive decision-support
tool via **Direct Lake** in Power BI.

The design mirrors how institutional research teams connect **macro regimes → asset-class →
sector → company** performance.

---

## Repository layout

```
macro-market-engine/
├─ fabric/                     # Fabric Git-synced items (import into a Fabric workspace)
│  ├─ MacroMarketLH.Lakehouse/        # Lakehouse item (Bronze/Silver/Gold Delta tables live here)
│  ├─ *.Notebook/                     # PySpark + Data Science notebooks (Fabric Git format)
│  ├─ PL_Daily_Orchestration.DataPipeline/   # Orchestration pipeline (JSON)
│  └─ MacroMarket.SemanticModel/      # Direct Lake semantic model (TMDL)
├─ src/mme/                    # Reusable, unit-testable Python core
│  ├─ ingestion/              # FRED / World Bank / IMF / Yahoo / SEC EDGAR clients
│  ├─ common/                 # calendar, transforms, as-of joins, idempotent Delta merge
│  └─ quality/                # pipeline-gated checks (mirrored in notebook 70)
├─ scripts/                   # run_local_bronze.py, build_mme_wheel.ps1
├─ powerbi/                   # MacroMarketTheme.json + BUILD_REPORT.md
├─ config/sources.yml         # Declarative catalog of every data source + series/tickers
├─ docs/                      # Architecture, lineage, governance, executive brief, runbook
├─ .env.example               # API keys & settings template (copy to .env, never commit .env)
└─ requirements.txt
```

## Medallion flow

| Layer  | Location (OneLake)            | Content |
|--------|-------------------------------|---------|
| Bronze | `Files/bronze/<source>/<ingestion_date>/` + `*_bronze` Delta | Raw JSON/Parquet, idempotent (watermark + MERGE) |
| Silver | `*_silver` Delta tables       | Cleaned, calendar-aligned, **as-of joined** (no look-ahead bias), log returns, rolling vol |
| Gold   | `Fact_*` / `Dim_*` Delta tables | Star schema + ML feature tables, V-Order + `OPTIMIZE ZORDER BY (DateKey)` |

## Analytical models (Fabric Data Science + MLflow)

1. **Regime detection** — HMM / GMM over Growth, Inflation, Financial Conditions → `Fact_Regime`.
2. **Macro-beta attribution** — rolling ElasticNet/Ridge + SHAP of excess returns vs macro surprises → `Fact_Macro_Beta`.
3. **Stress-test simulator** — VAR / Monte Carlo on regime-conditioned covariance → `Fact_Stress_Scenario`.

## Quick start

See [`docs/05_runbook.md`](docs/05_runbook.md) for the full end-to-end runbook. In short:

1. Create a **Fabric Trial** workspace and connect it to this repo via **Git integration** (folder: `macro-market-engine/fabric`).
2. Copy `.env.example` → `.env` and add a free [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html).
3. Run **PL_Daily_Orchestration** (ends with `70_Quality_Checks` governance gate).
4. Build the Power BI report from [`powerbi/BUILD_REPORT.md`](powerbi/BUILD_REPORT.md).

**Local:** `pip install -e ".[dev]"` → `pytest` → `python scripts/run_local_bronze.py`.

> **Note:** Fabric is a hosted SaaS platform. The items under `fabric/` are the Git-syncable
> definitions; the running Lakehouse, pipelines, MLflow experiments, and Direct Lake model are
> provisioned in the cloud workspace. The `src/mme` library can be run and unit-tested locally.
