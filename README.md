
# Finance Data Platform (ETL Hub)

A production-style financial data platform on the **Microsoft stack**: retail ETL (Azure Blob + Azure SQL + Power BI) and a **Microsoft Fabric** macro-to-market Lakehouse in [`macro-market-engine/`](macro-market-engine/).

## Features

- **Data ingestion**: Online Retail dataset, Exchange Rate API, optional CDC simulation
- **Transformation**: cleaning, multi-currency (GBP/USD/EUR/MAD), RFM, fraud + deep fraud ML
- **Warehouse**: star schema (Postgres local / SQLite fallback + optional **Azure SQL**)
- **Data lake**: optional **Azure Blob** archive of raw files
- **Dashboards**: Streamlit + Power BI docs
- **Fabric**: [`macro-market-engine`](macro-market-engine/) — FRED/markets, HMM regimes, Direct Lake semantic model

## Project structure

```text
FinanceETLHub/
├── azure/                 # Azure deployment guide
├── macro-market-engine/   # Microsoft Fabric Lakehouse (separate workload)
├── config/
├── src/
├── sql/postgresql/        # Local DW DDL
├── sql/azure/             # Azure SQL star schema
├── dashboards/
├── dags/
├── main.py
└── Makefile
```

## Setup

```bash
make setup
make up
make etl
```

Edit `.env` from `.env.example` (FX API key; optional Azure settings).

### Usage shortcuts

| Command | Action |
|---------|--------|
| `make cdc` | CDC simulation (initial + incremental) |
| `make etl` | Full pipeline |
| `make dashboard` | Streamlit |
| `make test` | pytest |

## Microsoft cloud (retail ETL)

1. Set `USE_AZURE=true` and connection strings in `.env` (see [`azure/README.md`](azure/README.md)).
2. Run `sql/azure/init_schema.sql` on your Azure SQL database.
3. `make etl` — mirrors loads to **Azure Blob** + **Azure SQL** when configured.

## Microsoft Fabric (macro analytics)

See [`macro-market-engine/docs/05_runbook.md`](macro-market-engine/docs/05_runbook.md). Git-sync folder `macro-market-engine/fabric` into a Fabric workspace.

## Orchestration

Airflow DAG: `dags/finance_etl_dag.py` (or replicate in **Azure Data Factory** / Fabric pipelines).

## Analytics & BI

- **Streamlit:** `make dashboard`
- **Power BI:** PostgreSQL, Azure SQL, or Fabric Direct Lake — see `dashboards/powerbi/README.md`