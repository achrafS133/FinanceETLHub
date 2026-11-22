# Power BI Report Design (5 pages)

Step-by-step build checklist: [`powerbi/BUILD_REPORT.md`](../powerbi/BUILD_REPORT.md).  
Theme: [`powerbi/MacroMarketTheme.json`](../powerbi/MacroMarketTheme.json).

Connect a report to the **MacroMarket** Direct Lake semantic model. Institutional UI/UX:
muted palette (slate/ink + one accent), clear KPI hierarchy, generous whitespace, consistent
number formats. All measures live in the `_Measures` table.

## Shared elements
- **Theme:** neutral grays, single accent (e.g. deep teal); semantic colors for regimes
  (Goldilocks=green, Reflation=amber, Stagflation=red, Contraction=gray).
- **Slicers (sync across pages):** Time horizon (1Y/5Y/10Y/Max via `Dim_Date`), Regime (`Fact_Regime[RegimeName]`).

## Page 1 — Global Macro Regime Monitor (exec summary)
| Visual | Fields / measures |
|--------|-------------------|
| Regime KPI banner | `[Current Regime]` card |
| Yield-curve recession indicator | line of `Fact_Macro_Monthly[value]` where `SeriesId="T10Y2Y"` over `Dim_Date[Date]`, reference line at 0 |
| Inflation vs policy rate (dual axis) | CPI YoY (`[Macro YoY %]`, SeriesId=CPIAUCSL) vs Fed Funds (SeriesId=FEDFUNDS) |
| Global choropleth | `Dim_Country` + World Bank GDP growth / Debt-to-GDP |

## Page 2 — Market & Sector Impact Matrix
| Visual | Fields / measures |
|--------|-------------------|
| Regime × Sector heatmap | matrix: rows `Dim_Asset[GICSSector]`, cols `Fact_Regime[RegimeName]`, values `[Annualized Return]` / `[Sharpe Ratio]` |
| Risk vs Return scatter | x `[Annualized Volatility]`, y `[Annualized Return]`, legend `Dim_Asset[AssetClass]` |
| Rolling equity–bond correlation | line chart (SPY vs AGG rolling corr; add a measure or precompute) |
| Cross-filter | click a regime cell → filters the scatter + best/worst sector cards |

## Page 3 — Corporate Fundamental Resilience (SEC EDGAR)
| Visual | Fields / measures |
|--------|-------------------|
| Operating margin vs CPI | sector operating margin (`Fact_Company_Quarterly`) vs CPI trend |
| Debt/Equity vs rate sensitivity | scatter: x Debt-to-Equity, y macro beta to `DGS10_chg` (`Fact_Macro_Beta`) |
| Drill-through table | per-company `Fact_Company_Quarterly` details |

## Page 4 — Interactive Portfolio Stress-Tester
Use **What-If parameters** (Modeling → New parameter → Numeric range) created in the report:
- `Equities %`, `Bonds %`, `Gold %`, `Tech %` (0–100, step 5).
- A **Scenario** slicer bound to `Fact_Stress_Scenario[Scenario]`.

Portfolio outcome measures (report-level, referencing the What-If params + `Fact_Stress_Scenario`):

```DAX
Portfolio Expected Drawdown =
VAR wE = 'Equities %'[Equities % Value] / 100
VAR wB = 'Bonds %'[Bonds % Value] / 100
VAR wG = 'Gold %'[Gold % Value] / 100
RETURN
    SUMX (
        VALUES ( Fact_Stress_Scenario[Ticker] ),
        VAR ac = RELATED ( Dim_Asset[AssetClass] )
        VAR w = SWITCH ( ac, "Fixed Income", wB, "Commodity", wG, wE )
        RETURN w * CALCULATE ( AVERAGE ( Fact_Stress_Scenario[ExpectedDrawdown] ) )
    )
```

Cards: Projected Drawdown, 10-day VaR (95%), Median Recovery (days), using the scenario-weighted
measures above. Add a waterfall of each sleeve's contribution to drawdown.

## Page 5 — Data Quality & Governance

| Visual | Fields / measures |
|--------|-------------------|
| KPI — failed checks | `[Latest Run Failures]` or count rows where `status = fail` on max `run_timestamp` |
| Table | `Gold_Audit_Quality_Checks` (all columns), sort `run_timestamp` desc |
| Matrix | Rows `check_name`, Values count of `status`, filter last 7 `ingestion_date` values |
| Conditional formatting | `status`: pass=green, fail=red, skip=gray |
