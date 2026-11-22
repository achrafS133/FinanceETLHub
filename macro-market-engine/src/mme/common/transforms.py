"""Stationarity & cleaning transforms for financial time-series.

All functions are pandas-based for unit testing; the PySpark notebooks mirror
this exact logic with window functions for scale.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def to_datekey(s: pd.Series) -> pd.Series:
    """Convert a datetime series to integer yyyymmdd DateKey."""
    return pd.to_datetime(s).dt.strftime("%Y%m%d").astype(int)


def log_returns(prices: pd.Series) -> pd.Series:
    r"""Log returns r_t = ln(P_t / P_{t-1})."""
    return np.log(prices / prices.shift(1))


def simple_returns(prices: pd.Series) -> pd.Series:
    return prices.pct_change()


def yoy_pct(values: pd.Series, periods_per_year: int = 12) -> pd.Series:
    """Year-over-year % change (default monthly series -> 12-period lag)."""
    return values.pct_change(periods=periods_per_year) * 100.0


def mom_pct(values: pd.Series) -> pd.Series:
    return values.pct_change() * 100.0


def rolling_vol(returns: pd.Series, window: int = 30, annualize: int = 252) -> pd.Series:
    """Rolling realized volatility, annualized (sqrt of trading days)."""
    return returns.rolling(window, min_periods=max(2, window // 2)).std() * np.sqrt(annualize)


def winsorize(s: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    """Clip a series to its [lower, upper] quantiles to tame bad-tick anomalies."""
    lo, hi = s.quantile(lower), s.quantile(upper)
    return s.clip(lower=lo, upper=hi)


def flag_outliers(returns: pd.Series, z: float = 8.0) -> pd.Series:
    """Boolean flag where |z-score| of a daily return exceeds ``z`` (bad-tick suspect)."""
    mu, sd = returns.mean(), returns.std(ddof=0)
    if sd == 0 or np.isnan(sd):
        return pd.Series(False, index=returns.index)
    return (returns - mu).abs() / sd > z
