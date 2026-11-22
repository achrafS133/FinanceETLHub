# Runbook — stand up the engine in Fabric

## Prerequisites

- A Microsoft **Fabric Trial** (60-day free capacity) or any Fabric capacity.
- A free **FRED API key** — https://fred.stlouisfed.org/docs/api/api_key.html
- A descriptive **SEC EDGAR User-Agent** (your name + email).
- A GitHub repo containing this project (for Fabric Git integration).

## Step 1 — Create the workspace & Lakehouse

1. Fabric portal → **Workspaces** → New → assign Trial capacity.
2. Create a **Lakehouse** named exactly `MacroMarketLH` (matches the notebook metadata).

## Step 2 — Connect Git

1. Workspace → **Settings → Git integration** → connect your GitHub repo.
2. Set the **Git folder** to `macro-market-engine/fabric`.
3. **Update all** to pull the notebooks, pipeline, and semantic model into the workspace.
   - On first sync, Fabric assigns real GUIDs. Open `PL_Daily_Orchestration` and re-point each
     notebook activity if the placeholder GUIDs didn't auto-bind.
   - Open the `MacroMarket` semantic model → `expressions.tmdl` equivalent in the UI and set the
     **SQL analytics endpoint** + Lakehouse GUID (Lakehouse → Settings → SQL analytics endpoint).

## Step 3 — Secrets

Preferred (production): store `FRED-API-KEY` and `SEC-USER-AGENT` in Azure Key Vault and set the
vault URL in `10_Bronze_Ingest`. Quick start: pass them as pipeline parameters, or run notebook
10 interactively and paste into the parameter cell.

## Step 4 — Make the `mme` library importable

Pick one:
- **Environment (recommended):** `.\scripts\build_mme_wheel.ps1` then upload `dist/*.whl` to a Fabric
  **Environment**, attach to all notebooks; or
- **Resources folder:** copy `src/mme` into the notebook's built-in **Resources** and the
  `sys.path.append("/lakehouse/default/Files/lib")` line will pick it up (upload `src` to
  `Files/lib`).

## Step 5 — Run (in order, or just run the pipeline)

```
00 → 10 → 20 → 30 → 40/50 → 60 → 70_Quality_Checks
```
Or trigger **PL_Daily_Orchestration** once; schedule it daily afterwards.

**Direct Lake:** no import refresh in the pipeline. If you add Gold columns/tables, open the semantic
model in Fabric and run **Refresh metadata** once (schema change only).

## Step 6 — Semantic model & report

1. Confirm the `MacroMarket` Direct Lake model shows all Gold tables (including `Gold_Audit_Quality_Checks`).
2. Verify DAX in `_Measures` (`Cumulative Equity`, `Max Drawdown` without EARLIER, regime KPIs).
3. Build the report using [`powerbi/BUILD_REPORT.md`](../powerbi/BUILD_REPORT.md) + theme JSON.

## Local development (optional, no Fabric)

```powershell
cd macro-market-engine
python -m venv .venv ; .\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest
python scripts/run_local_bronze.py   # writes data/bronze/*/ingestion_date=.../data.parquet
```
Set `FRED_API_KEY` / `SEC_USER_AGENT` in `.env` (copy from `.env.example`).

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `bronze_fred` empty | `FRED_API_KEY` not set — check Key Vault/params. |
| `bronze_sec_edgar` skipped | `SEC_USER_AGENT` missing (EDGAR blocks blank UA). |
| Pipeline notebook activity fails to bind | Re-select the notebook in the activity after Git sync. |
| Direct Lake shows no data | Set the SQL endpoint/Lakehouse GUID in the model's `DatabaseQuery` expression. |
| yfinance throttled | The client auto-falls back to Stooq; or re-run later. |
| Pipeline fails on 70 | Open `Gold_Audit_Quality_Checks` — failed `check_name` + `detail`. |
| First-run drift `skip` | Expected until `Gold_Audit_Run_Metrics` has a prior snapshot. |
