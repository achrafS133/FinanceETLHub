# Data Quality & Governance Report

## 1. Validation checks (enforced in production)

Checks run in Fabric notebook **`70_Quality_Checks`** (final activity in `PL_Daily_Orchestration`).
Implementation: [`src/mme/quality/checks.py`](../src/mme/quality/checks.py) (Spark wrappers + pandas unit tests).

| # | Check | Layer | Notebook check id |
|---|-------|-------|-------------------|
| 1 | No nulls on key columns | Silver | `non_null_market_silver`, `non_null_macro_silver` |
| 2 | No duplicate `(Ticker, DateKey)` | Gold | `unique_fact_market` |
| 3 | No look-ahead (`effective ≥ release ≥ obs`) | Silver | `no_lookahead_silver` |
| 4 | `adj_close > 0`, `volume ≥ 0` | Silver | `positive_prices_silver` |
| 5 | Outlier rate `< 1%` per ticker | Silver | `outlier_rate_silver` |
| 6 | Row-count drift `≤ 15%` vs prior run | Gold/Silver | `row_drift_*` |
| 7 | `RegimeName` in allowed HMM labels | Model | `regime_labels_valid` |
| 8 | Fact tickers ⊆ `Dim_Asset` | Gold | `referential_*` |

On **failure**: notebook raises `RuntimeError` → pipeline activity fails.  
On **every run**: one row per check is appended to **`Gold_Audit_Quality_Checks`**.

### Audit table schema — `Gold_Audit_Quality_Checks`

| Column | Type | Description |
|--------|------|-------------|
| `check_name` | string | Stable id (e.g. `no_lookahead_silver`) |
| `status` | string | `pass` \| `fail` \| `skip` |
| `metric_value` | double | Violation count or rate (drift %, worst outlier rate, …) |
| `detail` | string | Human-readable message |
| `ingestion_date` | string | Business date of the run |
| `run_timestamp` | string | UTC ISO timestamp |

Exposed in the **MacroMarket** semantic model for Power BI Page 5 (see [`powerbi/BUILD_REPORT.md`](../powerbi/BUILD_REPORT.md)).

### Row-count baseline — `Gold_Audit_Run_Metrics`

| Column | Type |
|--------|------|
| `table_name` | string |
| `row_count` | long |
| `run_timestamp` | string |
| `ingestion_date` | string |

Used by check #6 on the **next** pipeline run (first run skips drift with `skip` status).

### Local / CI testing

Pandas mirrors of core logic: `tests/test_quality_checks.py`. As-of bias: `tests/test_asof_and_transforms.py`.

Great Expectations remains **optional** in `requirements.txt`; the Delta audit trail is the default governance artifact.

## 2. Schema-drift strategy

- Bronze uses `mergeSchema` only with explicit review; Silver/Gold use `overwriteSchema=true`
  on controlled rebuilds. New FRED series or tickers are added via `config/sources.yml`
  (declarative) so schema changes are reviewable in a PR diff.

## 3. Governance

- **Secrets**: FRED key and SEC User-Agent from Key Vault / `.env` (git-ignored).
- **Config**: `config/sources.yml` — single declarative catalog.
- **Lineage**: Bronze audit columns; Fabric item lineage; Git-versioned `fabric/`.
- **CI**: `.github/workflows/mme-tests.yml` runs pytest on `macro-market-engine/**` changes.

## 4. Capacity & cost monitoring

| Item | Avg CU-s / run | Notes |
|------|----------------|-------|
| 10_Bronze_Ingest | _TBD_ | network-bound |
| 20_Silver_Align | _TBD_ | shuffle-heavy |
| 30_Gold_StarSchema | _TBD_ | OPTIMIZE/ZORDER |
| 40/50/60 ML | _TBD_ | sklearn/hmmlearn |
| 70_Quality_Checks | _TBD_ | light aggregates |