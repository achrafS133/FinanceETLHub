import pandas as pd

from mme.quality import (
    ALLOWED_REGIME_LABELS,
    assert_no_lookahead_pandas,
    assert_non_null_pandas,
    assert_outlier_rate_pandas,
    assert_referential_pandas,
    assert_regime_labels_pandas,
    assert_row_count_drift,
    assert_unique_pandas,
)


def test_assert_non_null_pass_and_fail():
    ok = pd.DataFrame({"DateKey": [1, 2], "Ticker": ["SPY", "AGG"]})
    assert assert_non_null_pandas("t", ok, ["DateKey", "Ticker"]).status == "pass"
    bad = ok.copy()
    bad.loc[0, "Ticker"] = None
    assert assert_non_null_pandas("t", bad, ["Ticker"]).status == "fail"


def test_assert_unique():
    df = pd.DataFrame({"Ticker": ["A", "A", "B"], "DateKey": [1, 1, 2]})
    assert assert_unique_pandas("t", df, ["Ticker", "DateKey"]).status == "fail"
    assert assert_unique_pandas("t", df.drop_duplicates(), ["Ticker", "DateKey"]).status == "pass"


def test_assert_no_lookahead_silver_macro():
    df = pd.DataFrame(
        {
            "obs_date": pd.to_datetime(["2024-01-31"]),
            "release_date": pd.to_datetime(["2024-02-15"]),
            "effective_date": pd.to_datetime(["2024-02-15"]),
        }
    )
    assert assert_no_lookahead_pandas("t", df).status == "pass"
    df.loc[0, "effective_date"] = pd.Timestamp("2024-01-01")
    assert assert_no_lookahead_pandas("t", df).status == "fail"


def test_assert_outlier_rate():
    df = pd.DataFrame({"ticker": ["SPY"] * 100, "is_outlier": [False] * 99 + [True]})
    assert assert_outlier_rate_pandas("t", df, max_rate=0.02).status == "pass"
    assert assert_outlier_rate_pandas("t", df, max_rate=0.005).status == "fail"


def test_assert_referential():
    fact = pd.DataFrame({"Ticker": ["SPY", "FAKE"]})
    dim = pd.DataFrame({"Ticker": ["SPY", "AGG"]})
    assert assert_referential_pandas("t", fact, dim, "Ticker", "Ticker").status == "fail"


def test_assert_regime_labels():
    df = pd.DataFrame({"RegimeName": list(ALLOWED_REGIME_LABELS)[:2]})
    assert assert_regime_labels_pandas("t", df).status == "pass"
    df2 = pd.DataFrame({"RegimeName": ["Unknown Regime"]})
    assert assert_regime_labels_pandas("t", df2).status == "fail"


def test_row_count_drift():
    assert assert_row_count_drift("t", 1000, 950, tolerance=0.15).status == "pass"
    assert assert_row_count_drift("t", 1000, 500, tolerance=0.15).status == "fail"
    assert assert_row_count_drift("t", 100, None).status == "skip"