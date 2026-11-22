"""World Bank Indicators API client (no key required)."""
from __future__ import annotations

import os
from typing import List, Optional

import pandas as pd
import requests


class WorldBankClient:
    def __init__(self, base_url: Optional[str] = None, session: Optional[requests.Session] = None):
        self.base_url = (base_url or os.getenv("WORLD_BANK_BASE_URL", "https://api.worldbank.org/v2")).rstrip("/")
        self.session = session or requests.Session()

    def _schema(self) -> pd.DataFrame:
        return pd.DataFrame(columns=["country_iso3", "indicator", "year", "value"])

    def get_indicator(self, indicator: str, countries: List[str], start: int = 1990, end: int = 2025) -> pd.DataFrame:
        """Annual panel for one indicator across many countries."""
        country_str = ";".join(countries)
        url = f"{self.base_url}/country/{country_str}/indicator/{indicator}"
        params = {"format": "json", "per_page": 20000, "date": f"{start}:{end}"}
        resp = self.session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
            return self._schema()
        rows = payload[1]
        df = pd.DataFrame(
            {
                "country_iso3": [r["countryiso3code"] for r in rows],
                "indicator": indicator,
                "year": [int(r["date"]) for r in rows],
                "value": [r["value"] for r in rows],
            }
        )
        return df.dropna(subset=["value"]).sort_values(["country_iso3", "year"]).reset_index(drop=True)

    def get_many(self, indicators: List[str], countries: List[str], **kw) -> pd.DataFrame:
        frames = [self.get_indicator(i, countries, **kw) for i in indicators]
        frames = [f for f in frames if not f.empty]
        return pd.concat(frames, ignore_index=True) if frames else self._schema()
