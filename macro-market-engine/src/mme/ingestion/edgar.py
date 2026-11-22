"""SEC EDGAR client for company fundamentals.

EDGAR requires a descriptive User-Agent header; requests without one are blocked.
Each financial fact carries ``filed`` (the SEC filing date) which is the correct
as-of key for aligning fundamentals to market data without look-ahead bias.
"""
from __future__ import annotations

import os
from typing import Dict, List, Optional

import pandas as pd
import requests

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
FACTS_URL = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik:010d}/us-gaap/{concept}.json"


class EdgarClient:
    def __init__(self, user_agent: Optional[str] = None, session: Optional[requests.Session] = None):
        self.user_agent = user_agent or os.getenv("SEC_USER_AGENT", "")
        self.session = session or requests.Session()
        if self.user_agent:
            self.session.headers.update({"User-Agent": self.user_agent})
        self._cik_map: Dict[str, int] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.user_agent)

    def _schema(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=["ticker", "cik", "concept", "end", "filed", "form", "fy", "fp", "value"]
        )

    def ticker_to_cik(self) -> Dict[str, int]:
        if self._cik_map or not self.enabled:
            return self._cik_map
        resp = self.session.get(TICKERS_URL, timeout=60)
        resp.raise_for_status()
        self._cik_map = {row["ticker"].upper(): int(row["cik_str"]) for row in resp.json().values()}
        return self._cik_map

    def get_concept(self, ticker: str, concept_tag: str, concept_label: str) -> pd.DataFrame:
        if not self.enabled:
            return self._schema()
        cik = self.ticker_to_cik().get(ticker.upper())
        if cik is None:
            return self._schema()
        url = FACTS_URL.format(cik=cik, concept=concept_tag)
        resp = self.session.get(url, timeout=60)
        if resp.status_code == 404:
            return self._schema()
        resp.raise_for_status()
        units = resp.json().get("units", {})
        facts = units.get("USD", [])
        if not facts:
            return self._schema()
        df = pd.DataFrame(facts)
        df = df[df["form"].isin(["10-Q", "10-K"])].copy()
        return pd.DataFrame(
            {
                "ticker": ticker.upper(),
                "cik": cik,
                "concept": concept_label,
                "end": pd.to_datetime(df["end"]),
                "filed": pd.to_datetime(df["filed"]),
                "form": df["form"].values,
                "fy": df.get("fy"),
                "fp": df.get("fp"),
                "value": pd.to_numeric(df["val"], errors="coerce"),
            }
        ).dropna(subset=["value"]).reset_index(drop=True)

    def get_many(self, tickers: List[str], concepts: Dict[str, str]) -> pd.DataFrame:
        """concepts: {label: us-gaap-tag}."""
        frames = []
        for t in tickers:
            for label, tag in concepts.items():
                f = self.get_concept(t, tag, label)
                if not f.empty:
                    frames.append(f)
        return pd.concat(frames, ignore_index=True) if frames else self._schema()
