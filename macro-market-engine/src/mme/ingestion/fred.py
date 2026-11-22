"""FRED (Federal Reserve Bank of St. Louis) client.

Captures BOTH the reference-period date (``obs_date``) and the first public
release date (``release_date``). The Silver layer must join macro to market
strictly on ``release_date`` to avoid look-ahead bias — a CPI print for the
March reference month is not knowable until mid-April.
"""
from __future__ import annotations

import os
from typing import List, Optional

import pandas as pd
import requests

FRED_BASE = "https://api.stlouisfed.org/fred"


class FredClient:
    def __init__(self, api_key: Optional[str] = None, session: Optional[requests.Session] = None):
        self.api_key = api_key or os.getenv("FRED_API_KEY", "")
        self.session = session or requests.Session()

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def _schema(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=["series_id", "obs_date", "release_date", "value"]
        ).astype({"series_id": "object", "value": "float64"})

    def get_series(self, series_id: str, start: str = "2000-01-01") -> pd.DataFrame:
        """Return tidy observations with first-release dates.

        Uses ``output_type=4`` (initial release only) so each observation carries
        the vintage date it first became public — the correct as-of key.
        """
        if not self.enabled:
            return self._schema()

        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start,
            "realtime_start": "1776-07-04",  # ask for full vintage history
            "realtime_end": "9999-12-31",
            "output_type": 4,                 # 4 = initial release values only
        }
        resp = self.session.get(f"{FRED_BASE}/series/observations", params=params, timeout=60)
        resp.raise_for_status()
        obs = resp.json().get("observations", [])
        if not obs:
            return self._schema()

        df = pd.DataFrame(obs)
        df = df[df["value"] != "."]  # FRED encodes missing as "."
        out = pd.DataFrame(
            {
                "series_id": series_id,
                "obs_date": pd.to_datetime(df["date"]),
                # realtime_start on an initial-release pull == first publication date
                "release_date": pd.to_datetime(df["realtime_start"]),
                "value": pd.to_numeric(df["value"], errors="coerce"),
            }
        ).dropna(subset=["value"])
        return out.sort_values("obs_date").reset_index(drop=True)

    def get_many(self, series_ids: List[str], start: str = "2000-01-01") -> pd.DataFrame:
        frames = [self.get_series(s, start=start) for s in series_ids]
        frames = [f for f in frames if not f.empty]
        return pd.concat(frames, ignore_index=True) if frames else self._schema()
