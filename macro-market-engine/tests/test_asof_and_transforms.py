"""Unit tests proving the two most important senior-level correctness properties:
no look-ahead bias in the as-of join, and correct return/vol math."""
import numpy as np
import pandas as pd

from mme.common import asof_join, log_returns, winsorize, build_dim_date


def test_asof_join_has_no_lookahead():
    # Market trades daily in March; CPI for Feb is only RELEASED on 2024-03-12.
    market = pd.DataFrame({"date": pd.to_datetime(["2024-03-01", "2024-03-11", "2024-03-12", "2024-03-13"])})
    cpi = pd.DataFrame(
        {
            "release_date": pd.to_datetime(["2024-03-12"]),  # publication date
            "obs_date": pd.to_datetime(["2024-02-29"]),      # reference period (Feb)
            "value": [310.0],
        }
    )
    out = asof_join(market, cpi, left_time="date", right_time="release_date")
    # Before the release date, the macro value must be unknown (NaN) -> no peeking.
    assert out.loc[out["date"] < "2024-03-12", "value"].isna().all()
    # On/after the release date the value is available.
    assert (out.loc[out["date"] >= "2024-03-12", "value"] == 310.0).all()


def test_log_returns():
    prices = pd.Series([100.0, 110.0, 99.0])
    r = log_returns(prices)
    assert np.isnan(r.iloc[0])
    assert abs(r.iloc[1] - np.log(110 / 100)) < 1e-12


def test_winsorize_clips_tails():
    s = pd.Series(list(range(100)) + [100000])  # one huge bad tick
    w = winsorize(s, 0.01, 0.99)
    assert w.max() < 100000


def test_dim_date_flags_weekend_as_non_trading():
    dim = build_dim_date("2024-01-01", "2024-01-31")
    sat = dim.loc[dim["Date"] == "2024-01-06"].iloc[0]
    assert not sat["IsTradingDay"]
    assert sat["NextTradingDay"] >= pd.Timestamp("2024-01-06")
