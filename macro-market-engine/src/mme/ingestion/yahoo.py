"""Market OHLCV client. Prefers yfinance; falls back to Stooq via pandas-datareader.

Always returns Adjusted Close (``adj_close``) so splits/dividends are handled
upstream of return calculations.
"""
from __future__ import annotations

from typing import List

import pandas as pd


class MarketClient:
    def _schema(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
        )

    def _from_yfinance(self, tickers: List[str], start: str) -> pd.DataFrame:
        import yfinance as yf  # imported lazily so tests/local runs don't require it

        raw = yf.download(
            tickers, start=start, auto_adjust=False, progress=False, group_by="ticker", threads=True
        )
        frames = []
        # yfinance returns a column MultiIndex (ticker, field) for multi-ticker pulls.
        for t in tickers:
            try:
                sub = raw[t] if isinstance(raw.columns, pd.MultiIndex) else raw
            except KeyError:
                continue
            if sub.dropna(how="all").empty:
                continue
            frames.append(
                pd.DataFrame(
                    {
                        "ticker": t,
                        "date": pd.to_datetime(sub.index),
                        "open": sub["Open"].values,
                        "high": sub["High"].values,
                        "low": sub["Low"].values,
                        "close": sub["Close"].values,
                        "adj_close": sub["Adj Close"].values,
                        "volume": sub["Volume"].values,
                    }
                )
            )
        return pd.concat(frames, ignore_index=True) if frames else self._schema()

    def _from_stooq(self, tickers: List[str], start: str) -> pd.DataFrame:
        from pandas_datareader import data as pdr  # lazy import

        frames = []
        for t in tickers:
            try:
                sub = pdr.DataReader(t, "stooq", start=start).sort_index()
            except Exception:
                continue
            if sub.empty:
                continue
            frames.append(
                pd.DataFrame(
                    {
                        "ticker": t,
                        "date": pd.to_datetime(sub.index),
                        "open": sub["Open"].values,
                        "high": sub["High"].values,
                        "low": sub["Low"].values,
                        "close": sub["Close"].values,
                        "adj_close": sub["Close"].values,  # Stooq close is already adjusted
                        "volume": sub["Volume"].values,
                    }
                )
            )
        return pd.concat(frames, ignore_index=True) if frames else self._schema()

    def get_prices(self, tickers: List[str], start: str = "2005-01-01") -> pd.DataFrame:
        try:
            df = self._from_yfinance(tickers, start)
            if not df.empty:
                return df.dropna(subset=["adj_close"]).reset_index(drop=True)
        except Exception:
            pass
        try:
            return self._from_stooq(tickers, start).dropna(subset=["adj_close"]).reset_index(drop=True)
        except Exception:
            return self._schema()
