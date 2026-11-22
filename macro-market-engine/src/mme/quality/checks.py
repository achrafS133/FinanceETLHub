"""Quality checks — pandas implementations (unit-tested) + Spark helpers for Fabric."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Set

import pandas as pd

ALLOWED_REGIME_LABELS: Set[str] = {
    "Goldilocks / Expansion",
    "Reflation",
    "Stagflation",
    "Contraction / Recession",
}


@dataclass
class CheckResult:
    check_name: str
    status: str  # "pass" | "fail" | "skip"
    metric_value: float
    detail: str = ""

    def raise_if_failed(self) -> None:
        if self.status == "fail":
            raise RuntimeError(f"[{self.check_name}] {self.detail} (metric={self.metric_value})")


def run_check(fn, name: str, *args, **kwargs) -> CheckResult:
    try:
        return fn(name, *args, **kwargs)
    except Exception as exc:  # noqa: BLE001 — surface any unexpected error as fail
        return CheckResult(name, "fail", float("nan"), str(exc))


# ---- Pandas (local tests + mirror of Spark logic) ----


def assert_non_null_pandas(
    name: str, df: pd.DataFrame, cols: Sequence[str]
) -> CheckResult:
    bad = 0
    for c in cols:
        if c not in df.columns:
            return CheckResult(name, "fail", float(bad), f"missing column {c}")
        bad += int(df[c].isna().sum())
    if bad > 0:
        return CheckResult(name, "fail", float(bad), f"{bad} nulls in {cols}")
    return CheckResult(name, "pass", 0.0, "ok")


def assert_unique_pandas(name: str, df: pd.DataFrame, keys: Sequence[str]) -> CheckResult:
    dup_groups = df.groupby(list(keys), dropna=False).size()
    dup = int((dup_groups > 1).sum())
    if dup > 0:
        return CheckResult(name, "fail", float(dup), f"{dup} duplicate key groups on {keys}")
    return CheckResult(name, "pass", 0.0, "ok")


def assert_no_lookahead_pandas(
    name: str,
    df: pd.DataFrame,
    *,
    obs_col: str = "obs_date",
    release_col: str = "release_date",
    effective_col: str = "effective_date",
) -> CheckResult:
    work = df.copy()
    for c in (obs_col, release_col, effective_col):
        if c in work.columns:
            work[c] = pd.to_datetime(work[c])
    bad = 0
    if release_col in work.columns and obs_col in work.columns:
        bad += int((work[release_col] < work[obs_col]).sum())
    if effective_col in work.columns and release_col in work.columns:
        bad += int((work[effective_col] < work[release_col]).sum())
    if bad > 0:
        return CheckResult(
            name, "fail", float(bad), f"{bad} rows violate release/obs/effective ordering"
        )
    return CheckResult(name, "pass", 0.0, "ok")


def assert_positive_prices_pandas(
    name: str, df: pd.DataFrame, price_col: str = "adj_close", volume_col: str = "volume"
) -> CheckResult:
    bad = 0
    if price_col in df.columns:
        bad += int((df[price_col] <= 0).sum())
    if volume_col in df.columns:
        bad += int((df[volume_col] < 0).sum())
    if bad > 0:
        return CheckResult(name, "fail", float(bad), f"{bad} invalid price/volume rows")
    return CheckResult(name, "pass", 0.0, "ok")


def assert_outlier_rate_pandas(
    name: str,
    df: pd.DataFrame,
    *,
    ticker_col: str = "ticker",
    flag_col: str = "is_outlier",
    max_rate: float = 0.01,
) -> CheckResult:
    if df.empty or flag_col not in df.columns:
        return CheckResult(name, "skip", 0.0, "no data or flag column")
    rates = df.groupby(ticker_col)[flag_col].mean()
    worst = float(rates.max()) if len(rates) else 0.0
    offenders = rates[rates > max_rate]
    if len(offenders) > 0:
        return CheckResult(
            name,
            "fail",
            worst,
            f"{len(offenders)} tickers exceed outlier rate {max_rate:.1%}; worst={worst:.2%}",
        )
    return CheckResult(name, "pass", worst, "ok")


def assert_referential_pandas(
    name: str,
    fact: pd.DataFrame,
    dim: pd.DataFrame,
    fact_key: str,
    dim_key: str,
) -> CheckResult:
    fset = set(fact[fact_key].dropna().astype(str))
    dset = set(dim[dim_key].dropna().astype(str))
    orphans = fset - dset
    n = len(orphans)
    if n > 0:
        sample = ", ".join(sorted(orphans)[:5])
        return CheckResult(name, "fail", float(n), f"{n} orphan keys e.g. {sample}")
    return CheckResult(name, "pass", 0.0, "ok")


def assert_regime_labels_pandas(
    name: str,
    df: pd.DataFrame,
    label_col: str = "RegimeName",
    allowed: Optional[Iterable[str]] = None,
) -> CheckResult:
    if df.empty or label_col not in df.columns:
        return CheckResult(name, "skip", 0.0, "no regime data")
    allowed_set = set(allowed or ALLOWED_REGIME_LABELS)
    bad = df[~df[label_col].isin(allowed_set)][label_col].unique()
    n = len(bad)
    if n > 0:
        return CheckResult(name, "fail", float(n), f"unknown labels: {list(bad)}")
    return CheckResult(name, "pass", 0.0, "ok")


def assert_row_count_drift(
    name: str, current: int, prior: Optional[int], tolerance: float = 0.15
) -> CheckResult:
    if prior is None or prior == 0:
        return CheckResult(name, "skip", float(current), "no prior row count baseline")
    drift = abs(current - prior) / prior
    if drift > tolerance:
        return CheckResult(
            name,
            "fail",
            drift,
            f"row count {current} vs prior {prior} drift {drift:.1%} > {tolerance:.1%}",
        )
    return CheckResult(name, "pass", drift, "ok")


# ---- Spark (Fabric notebook) ----


def spark_non_null(spark, table: str, cols: List[str], check_name: str) -> CheckResult:
    from pyspark.sql import functions as F

    df = spark.table(table)
    bad = 0
    for c in cols:
        bad += df.filter(F.col(c).isNull()).count()
    if bad:
        return CheckResult(check_name, "fail", float(bad), f"{bad} nulls in {cols}")
    return CheckResult(check_name, "pass", 0.0, "ok")


def spark_unique(spark, table: str, keys: List[str], check_name: str) -> CheckResult:
    dup = (
        spark.table(table)
        .groupBy(*keys)
        .count()
        .filter("count > 1")
        .count()
    )
    if dup:
        return CheckResult(check_name, "fail", float(dup), f"{dup} duplicate groups on {keys}")
    return CheckResult(check_name, "pass", 0.0, "ok")


def spark_no_lookahead(spark, table: str, check_name: str) -> CheckResult:
    from pyspark.sql import functions as F

    df = spark.table(table)
    bad = df.filter(
        (F.col("release_date") < F.col("obs_date"))
        | (F.col("effective_date") < F.col("release_date"))
    ).count()
    if bad:
        return CheckResult(check_name, "fail", float(bad), f"{bad} look-ahead rows")
    return CheckResult(check_name, "pass", 0.0, "ok")


def spark_positive_prices(spark, table: str, check_name: str) -> CheckResult:
    from pyspark.sql import functions as F

    df = spark.table(table)
    bad = df.filter((F.col("adj_close") <= 0) | (F.col("volume") < 0)).count()
    if bad:
        return CheckResult(check_name, "fail", float(bad), f"{bad} invalid price/volume rows")
    return CheckResult(check_name, "pass", 0.0, "ok")


def spark_outlier_rate(spark, table: str, check_name: str, max_rate: float = 0.01) -> CheckResult:
    pdf = (
        spark.table(table)
        .groupBy("ticker")
        .agg({"is_outlier": "avg"})
        .toPandas()
    )
    if pdf.empty:
        return CheckResult(check_name, "skip", 0.0, "empty")
    pdf.columns = ["ticker", "rate"]
    worst = float(pdf["rate"].max())
    if worst > max_rate:
        return CheckResult(
            check_name, "fail", worst, f"worst ticker outlier rate {worst:.2%} > {max_rate:.1%}"
        )
    return CheckResult(check_name, "pass", worst, "ok")


def spark_referential(
    spark, fact_table: str, dim_table: str, fact_key: str, dim_key: str, check_name: str
) -> CheckResult:
    orphans = (
        spark.table(fact_table)
        .select(fact_key)
        .distinct()
        .join(spark.table(dim_table).select(dim_key).distinct(), fact_key == dim_key, "left_anti")
        .count()
    )
    if orphans:
        return CheckResult(check_name, "fail", float(orphans), f"{orphans} orphan {fact_key}")
    return CheckResult(check_name, "pass", 0.0, "ok")


def spark_regime_labels(spark, table: str, check_name: str) -> CheckResult:
    allowed = list(ALLOWED_REGIME_LABELS)
    df = spark.table(table)
    bad = df.filter(~df["RegimeName"].isin(allowed)).select("RegimeName").distinct().count()
    if bad:
        return CheckResult(check_name, "fail", float(bad), "unknown RegimeName values")
    return CheckResult(check_name, "pass", 0.0, "ok")


def spark_row_count_drift(
    spark, table: str, audit_table: str, check_name: str, tolerance: float = 0.15
) -> CheckResult:
    from pyspark.sql import functions as F

    current = spark.table(table).count()
    prior = None
    if spark.catalog.tableExists(audit_table):
        hist = (
            spark.table(audit_table)
            .filter(F.col("table_name") == table)
            .orderBy(F.col("run_timestamp").desc())
        )
        rows = hist.limit(1).collect()
        if rows:
            prior = int(rows[0]["row_count"])
    return assert_row_count_drift(check_name, current, prior, tolerance)