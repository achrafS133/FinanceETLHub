# Architecture & Data Lineage

## End-to-end flow

```mermaid
flowchart LR
    subgraph Sources["Free, institutional-grade sources"]
        FRED["FRED REST API\n(macro + release dates)"]
        WB["World Bank API"]
        IMF["IMF DataMapper"]
        YF["Yahoo / Stooq\n(OHLCV)"]
        SEC["SEC EDGAR\n(10-Q / 10-K)"]
    end

    subgraph Ingest["Ingestion (Fabric)"]
        PL["Data Pipeline\nPL_Daily_Orchestration"]
        NB10["Notebook 10_Bronze_Ingest\n(PySpark + mme clients)"]
    end

    subgraph OneLake["OneLake · Medallion Lakehouse (MacroMarketLH)"]
        BRONZE["Bronze\nFiles/bronze/* + bronze_* Delta\n(idempotent MERGE)"]
        SILVER["Silver\nsilver_market_daily, silver_macro,\nsilver_macro_pit, silver_fundamentals\n(as-of joins, no look-ahead)"]
        GOLD["Gold Star Schema\nFact_* + Dim_* + Feature_Market_Macro\n(V-Order, ZORDER by DateKey)"]
    end

    subgraph DS["Fabric Data Science + MLflow"]
        HMM["40 · Regime HMM\n-> Fact_Regime"]
        BETA["50 · Macro Beta (ElasticNet+SHAP)\n-> Fact_Macro_Beta"]
        STRESS["60 · Stress Test (VAR/Monte Carlo)\n-> Fact_Stress_Scenario"]
    end

    subgraph QG["Quality gate"]
        NB70["70_Quality_Checks"]
        AUDIT["Gold_Audit_Quality_Checks\nGold_Audit_Run_Metrics"]
    end

    subgraph Serve["Serving"]
        SM["Direct Lake Semantic Model\nMacroMarket (TMDL)"]
        PBI["Power BI\n5-page report incl. governance"]
    end

    FRED & WB & IMF & YF & SEC --> NB10
    PL --> NB10
    NB10 --> BRONZE --> SILVER --> GOLD
    GOLD --> HMM & BETA
    HMM --> STRESS
    HMM & BETA & STRESS --> GOLD
    STRESS --> NB70
    BETA --> NB70
    NB70 --> AUDIT
    GOLD --> SM --> PBI
    AUDIT --> SM
```

## Layer responsibilities

| Layer | Items | Guarantees |
|-------|-------|-----------|
| **Bronze** | `Files/bronze/<source>/ingestion_date=.../`, `bronze_fred`, `bronze_market`, `bronze_world_bank`, `bronze_imf`, `bronze_sec_edgar` | Immutable raw copy + idempotent `MERGE` on natural keys (no duplicates on re-run). Audit columns `_source`, `_ingestion_date`, `_ingested_at`. |
| **Silver** | `silver_market_daily`, `silver_macro`, `silver_macro_pit`, `silver_fundamentals` | Adjusted-close returns, winsorized bad ticks, rolling vol; macro YoY/MoM; **release/filing-date as-of joins** → zero look-ahead bias; trading-day alignment. |
| **Gold** | Star schema facts/dims + ML outputs + `Gold_Audit_Quality_Checks`, `Gold_Audit_Run_Metrics` | Clean star schema, V-Order + `OPTIMIZE ZORDER BY (DateKey)` for Direct Lake; audit tables for governance reporting. |

## Key design decisions (why this is senior-level)

1. **Point-in-time correctness.** FRED is pulled with `output_type=4` (initial release) so each
   observation carries its *first publication date*. Market rows only ever see macro values
   already released — enforced by `merge_asof(direction="backward")` and the Spark forward-fill
   on `effective_date`. Proven by `tests/test_asof_and_transforms.py`.
2. **Mixed-frequency handling.** Monthly/quarterly macro is never interpolated with future values;
   it is forward-filled strictly from the release date and aligned to the next trading day.
3. **Idempotency.** Every Bronze write is a keyed Delta `MERGE`, so pipeline re-runs and backfills
   are safe.
4. **Direct Lake, not import.** The semantic model reads Gold Delta directly (no data duplication,
   low latency), which is the whole point of the Fabric serving layer.
