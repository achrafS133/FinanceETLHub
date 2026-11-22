"""As-of join that eliminates look-ahead bias.

The cardinal rule for mixed-frequency macro/market data: a daily market row dated
``t`` may only "see" a macro value whose *release/filing date* is <= ``t``. We never
forward-fill using the reference period (which is knowable only in hindsight).

``pandas.merge_asof`` with ``direction="backward"`` on the release date implements
exactly this: each trading day picks up the most recently *published* macro value.
"""
from __future__ import annotations

import pandas as pd


def asof_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_time: str,
    right_time: str,
    by: list[str] | None = None,
    suffix: str = "_macro",
) -> pd.DataFrame:
    """Backward as-of join.

    Parameters
    ----------
    left        : high-frequency frame (e.g. daily market), sorted ascending by ``left_time``.
    right       : low-frequency frame carrying a publication date (``right_time``).
    left_time   : column in ``left`` that is the point-in-time you "know" things (trading date).
    right_time  : column in ``right`` that is the publication/release/filing date.
    by          : optional keys to match within groups (e.g. ['country_iso3']).
    """
    left = left.sort_values(left_time).copy()
    right = right.sort_values(right_time).copy()
    merged = pd.merge_asof(
        left,
        right,
        left_on=left_time,
        right_on=right_time,
        by=by,
        direction="backward",
        suffixes=("", suffix),
    )
    return merged
