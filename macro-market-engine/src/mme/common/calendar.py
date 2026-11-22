"""Master calendar / Dim_Date builder.

Flags US trading days (NYSE-style: weekdays minus US market holidays), month-ends,
and quarter-ends. Macro releases that land on a non-trading day are aligned to the
*next* trading day by the Silver layer using ``next_trading_day``.
"""
from __future__ import annotations

import pandas as pd


def _us_market_holidays(years: range) -> set:
    """Approximate NYSE holiday set using pandas USFederalHolidayCalendar plus
    Good Friday. Good enough for alignment; swap in ``pandas_market_calendars``
    for exchange-exact sessions in production."""
    from pandas.tseries.holiday import USFederalHolidayCalendar, GoodFriday

    cal = USFederalHolidayCalendar()
    start = f"{min(years)}-01-01"
    end = f"{max(years)}-12-31"
    fed = set(cal.holidays(start=start, end=end))
    # NYSE also observes Good Friday (not a US federal holiday). GoodFriday is a
    # ready-made Holiday rule in pandas, so we can enumerate its dates directly.
    gf = set(GoodFriday.dates(start, end))
    return fed | gf


def build_dim_date(start: str = "2000-01-01", end: str = "2030-12-31") -> pd.DataFrame:
    """Return a Dim_Date DataFrame keyed by integer ``DateKey`` (yyyymmdd)."""
    dates = pd.date_range(start, end, freq="D")
    years = range(dates.min().year, dates.max().year + 1)
    holidays = _us_market_holidays(years)

    df = pd.DataFrame({"Date": dates})
    df["DateKey"] = df["Date"].dt.strftime("%Y%m%d").astype(int)
    df["Year"] = df["Date"].dt.year
    df["Quarter"] = df["Date"].dt.quarter
    df["Month"] = df["Date"].dt.month
    df["MonthName"] = df["Date"].dt.strftime("%b")
    df["Day"] = df["Date"].dt.day
    df["DayOfWeek"] = df["Date"].dt.dayofweek  # 0=Mon
    df["IsWeekday"] = df["DayOfWeek"] < 5
    df["IsHoliday"] = df["Date"].isin(holidays)
    df["IsTradingDay"] = df["IsWeekday"] & ~df["IsHoliday"]
    df["IsMonthEnd"] = df["Date"].dt.is_month_end
    df["IsQuarterEnd"] = df["Date"].dt.is_quarter_end

    # next_trading_day: for any date, the first trading day on/after it.
    trading = df.loc[df["IsTradingDay"], "Date"].reset_index(drop=True)
    idx = trading.searchsorted(df["Date"].values, side="left")
    idx = idx.clip(max=len(trading) - 1)
    df["NextTradingDay"] = trading.iloc[idx].values
    return df
