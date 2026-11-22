# Build the MacroMarket report (~30 minutes)

## Connect to the model

1. In **Fabric** or **Power BI Desktop**, create a **Report** on the `MacroMarket` semantic model (Direct Lake).
2. **View → Themes → Browse for themes** → import [`MacroMarketTheme.json`](MacroMarketTheme.json).
3. Confirm relationships: `Dim_Date` ↔ facts; `Dim_Asset` ↔ market/beta/stress facts.

## Synced slicers (all pages)

- **Time horizon:** `Dim_Date[Date]` — use a relative date filter or buttons (1Y / 5Y / 10Y / All).
- **Regime:** `Fact_Regime[RegimeName]` — sync across pages.

## Page 1 — Global Macro Regime Monitor

| Visual | Fields |
|--------|--------|
| KPI card | `[Current Regime]` |
| Line | `Dim_Date[Date]`, `Fact_Macro_Monthly[value]` filtered `SeriesId = T10Y2Y`, constant line Y=0 |
| Dual-axis line | CPI YoY + Fed Funds (`Fact_Macro_Monthly`, two `SeriesId` filters) |
| Map | `Dim_Country` + World Bank fields (if modeled) or table by region |

## Page 2 — Market & Sector Impact Matrix

| Visual | Fields |
|--------|--------|
| Matrix heatmap | Rows `Dim_Asset[GICSSector]`, Cols `Fact_Regime[RegimeName]`, Values `[Annualized Return]`, `[Sharpe Ratio]` |
| Scatter | X `[Annualized Volatility]`, Y `[Annualized Return]`, Legend `Dim_Asset[AssetClass]` |
| Line | Rolling equity–bond correlation (custom measure or precomputed) |

Enable **Edit interactions** so matrix regime cell cross-filters the scatter.

## Page 3 — Corporate Fundamental Resilience

Requires `Fact_Company_Quarterly` populated (`SEC_USER_AGENT` set).

| Visual | Fields |
|--------|--------|
| Combo | Sector margin vs CPI |
| Scatter | Debt vs `[Avg Macro Beta]` where `Factor` = DGS10_chg |
| Table | Drill-through company detail |

## Page 4 — Portfolio Stress-Tester

**Modeling → New parameter** (numeric, 0–100, step 5):

- `Equities %`, `Bonds %`, `Gold %`, `Tech %`

**Slicer:** `Fact_Stress_Scenario[Scenario]`

Use measures from [`docs/06_powerbi_report.md`](../docs/06_powerbi_report.md) for portfolio-weighted drawdown/VaR.

Cards: `[Scenario Expected Drawdown]`, `[Scenario VaR 95% (10d)]`, `[Scenario Recovery (days)]` with portfolio DAX.

## Page 5 — Data Quality & Governance

| Visual | Fields |
|--------|--------|
| KPI | `[Latest Run Failures]` from `Gold_Audit_Quality_Checks` (or card: `COUNTROWS` where status=fail on latest `run_timestamp`) |
| Table | `check_name`, `status`, `metric_value`, `detail`, `run_timestamp` |
| Filter | Latest `run_timestamp` only (top N by time) |
| Matrix | `check_name` × `status` counts over last 7 runs |

**Tip:** After the first successful pipeline run, this page proves governance for interviews.

## What-If parameters (Page 4)

See portfolio DAX in [`docs/06_powerbi_report.md`](../docs/06_powerbi_report.md).

## Demo video (3 min)

Script: start on Page 1 regime banner → Page 2 stagflation sector heatmap → Page 4 rate-shock scenario on 60/40 → end on Page 5 all-green checks.