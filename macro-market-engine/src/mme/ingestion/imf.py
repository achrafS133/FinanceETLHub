"""IMF DataMapper API client (WEO projections, no key required)."""
from __future__ import annotations

import os
from typing import List, Optional

import pandas as pd
import requests


class ImfClient:
    def __init__(self, base_url: Optional[str] = None, session: Optional[requests.Session] = None):
        self.base_url = (base_url or os.getenv("IMF_BASE_URL", "https://www.imf.org/external/datamapper/api/v1")).rstrip("/")
        self.session = session or requests.Session()

    def _schema(self) -> pd.DataFrame:
        return pd.DataFrame(columns=["country_iso3", "indicator", "year", "value"])

    def get_indicator(self, indicator: str, countries: Optional[List[str]] = None) -> pd.DataFrame:
        url = f"{self.base_url}/{indicator}"
        if countries:
            url = f"{url}/{'/'.join(countries)}"
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        values = resp.json().get("values", {}).get(indicator, {})
        records = []
        for iso3, by_year in values.items():
            for year, val in by_year.items():
                records.append((iso3, indicator, int(year), val))
        if not records:
            return self._schema()
        df = pd.DataFrame(records, columns=["country_iso3", "indicator", "year", "value"])
        return df.dropna(subset=["value"]).sort_values(["country_iso3", "year"]).reset_index(drop=True)

    def get_many(self, indicators: List[str], countries: Optional[List[str]] = None) -> pd.DataFrame:
        frames = [self.get_indicator(i, countries) for i in indicators]
        frames = [f for f in frames if not f.empty]
        return pd.concat(frames, ignore_index=True) if frames else self._schema()
