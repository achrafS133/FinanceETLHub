"""Data-quality assertions shared by tests and the Fabric 70_Quality_Checks notebook."""

from .checks import (
    ALLOWED_REGIME_LABELS,
    CheckResult,
    assert_no_lookahead_pandas,
    assert_non_null_pandas,
    assert_outlier_rate_pandas,
    assert_positive_prices_pandas,
    assert_referential_pandas,
    assert_regime_labels_pandas,
    assert_row_count_drift,
    assert_unique_pandas,
    run_check,
)

__all__ = [
    "ALLOWED_REGIME_LABELS",
    "CheckResult",
    "assert_no_lookahead_pandas",
    "assert_non_null_pandas",
    "assert_outlier_rate_pandas",
    "assert_positive_prices_pandas",
    "assert_referential_pandas",
    "assert_regime_labels_pandas",
    "assert_row_count_drift",
    "assert_unique_pandas",
    "run_check",
]