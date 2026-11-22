# Executive Research Brief (template)

> *Vanguard-style 2-page market commentary. Fill the bracketed values from your own model
> outputs after a full pipeline run; the structure and framing are provided.*

---

## Global Macro-to-Market Impact Engine — Market Commentary

**As of:** [DATE] · **Detected regime:** [`Current Regime` measure, e.g. "Stagflation"]

### 1. Where we are (the regime call)

Our Hidden Markov Model, trained on growth (industrial production), inflation (CPI YoY), and
financial conditions (10Y-2Y spread + high-yield credit spreads), currently classifies the
macro environment as **[regime]** with the yield curve at **[T10Y2Y value] bps** and core
inflation running **[CPI YoY]%**. The model has spent **[X]%** of the last decade in this regime.

### 2. What has worked in this regime (historical evidence)

Across all historical months labelled **[regime]**, the best risk-adjusted performers were:

| Sector / Asset | Ann. Return | Sharpe | Max Drawdown |
|----------------|-------------|--------|--------------|
| [XLE] | [..]% | [..] | [..]% |
| [XLV] | [..]% | [..] | [..]% |
| [GLD] | [..]% | [..] | [..]% |

…while **[worst sectors]** lagged. *(Source: Page 2, Market & Sector Impact Matrix.)*

### 3. Macro sensitivities (what's most at risk)

The ElasticNet macro-beta model shows the highest sensitivity to a **+100 bps rate move** in
**[sector]** (β = [..]) and to **USD strength** in **[sector]** (β = [..]). SHAP attribution
confirms **[factor]** as the dominant driver of recent sector dispersion.

### 4. Scenario stress test

Under a **[2022 Rate Shock / 2008 Credit Crunch]** scenario conditioned on the current regime,
a classic **60/40 portfolio** shows a modelled expected drawdown of **[..]%**, 10-day 95% VaR of
**[..]%**, and a median recovery of **[..] trading days**. Tilting **[+X% to Gold / −Y% to Tech]**
reduces modelled drawdown to **[..]%**.

### 5. A concrete insight (the demo narrative)

> *Example:* "The 2022–2024 rate cycle flipped the equity–bond correlation from **[−0.3]** to
> **[+0.4]**, erasing the diversification benefit of the 60/40 and explaining why both legs fell
> together. In our stress simulator, a 10% Gold sleeve cut peak drawdown by **[..] pts**."

### 6. Methodology & limitations

- **Point-in-time discipline:** all macro/market joins are as-of on release dates — results are
  free of look-ahead bias.
- **Limitations:** ETF proxies approximate asset classes; HMM regimes are latent and can relabel
  as new data arrives; Monte Carlo assumes (regime-conditioned) Gaussian returns and therefore
  understates tail risk; fundamentals cover a seed universe, not the full S&P 500.
- **Not investment advice.** Educational/portfolio demonstration only.
