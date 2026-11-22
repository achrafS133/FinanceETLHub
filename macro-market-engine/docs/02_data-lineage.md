# Data Lineage (table-level)

| Source | Bronze | Silver | Gold | Model output |
|--------|--------|--------|------|--------------|
| FRED series | `bronze_fred` (series_id, obs_date, release_date, value) | `silver_macro` (YoY/MoM, effective_date), `silver_macro_pit` (daily forward-fill) | `Fact_Macro_Monthly`, `Feature_Market_Macro` | `Fact_Regime`, `Fact_Macro_Beta` |
| World Bank | `bronze_world_bank` | (joins into country dims) | `Dim_Country` enrichment | — |
| IMF | `bronze_imf` | — | `Dim_Country` enrichment | — |
| Yahoo/Stooq | `bronze_market` | `silver_market_daily` (log returns, vol, winsorized) | `Fact_Market_Daily`, `Feature_Market_Macro` | `Fact_Stress_Scenario` |
| SEC EDGAR | `bronze_sec_edgar` | `silver_fundamentals` (filing-date aligned) | `Fact_Company_Quarterly` | — |
| (derived) | — | — | `Dim_Date`, `Dim_Asset`, `Dim_Macro_Indicator` | — |

## Natural keys (MERGE idempotency)

| Bronze table | Business key |
|--------------|--------------|
| `bronze_fred` | `(series_id, obs_date)` |
| `bronze_market` | `(ticker, date)` |
| `bronze_world_bank` | `(country_iso3, indicator, year)` |
| `bronze_imf` | `(country_iso3, indicator, year)` |
| `bronze_sec_edgar` | `(ticker, concept, end, filed)` |

## Column-level highlights

- `silver_market_daily.log_ret_w` — winsorized log return `ln(P_t/P_{t-1})`, `[1%,99%]` clipped per ticker.
- `silver_macro.effective_date` — release date shifted to the next trading day (look-ahead-safe join key).
- `silver_macro_pit.macro_value` — last *released* value as of each trading day (as-of join result).
- `Feature_Market_Macro.excess_ret` — `log_ret_w − Fed-Funds daily proxy`; the ML target.
- `Fact_Regime.RegimeName` — HMM latent-state label (Goldilocks / Reflation / Stagflation / Contraction).
