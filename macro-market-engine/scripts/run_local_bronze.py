#!/usr/bin/env python3
"""Fetch bronze-layer sources locally and write Parquet under data/bronze/ (gitignored)."""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

# Project root = parent of scripts/
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv

from mme.ingestion import (
    EdgarClient,
    FredClient,
    ImfClient,
    MarketClient,
    WorldBankClient,
    load_sources,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Local bronze ingestion for Macro-to-Market Engine")
    parser.add_argument("--start", default="2005-01-01")
    parser.add_argument("--out", default=None, help="Output root (default: ./data/bronze)")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    cfg = load_sources()
    day = dt.date.today().isoformat()
    out_root = Path(args.out or os.getenv("LOCAL_LAKE_ROOT", ROOT / "data")) / "bronze"

    def write(name: str, df):
        if df is None or df.empty:
            print(f"[{name}] skipped (empty)")
            return
        path = out_root / name / f"ingestion_date={day}"
        path.mkdir(parents=True, exist_ok=True)
        out_file = path / "data.parquet"
        df.to_parquet(out_file, index=False)
        print(f"[{name}] wrote {len(df)} rows → {out_file}")

    fred = FredClient()
    if fred.enabled:
        write("fred", fred.get_many(list(cfg["fred"]["series"].keys()), start=args.start))
    else:
        print("[fred] skipped — set FRED_API_KEY in .env")

    wb = WorldBankClient()
    write(
        "world_bank",
        wb.get_many(list(cfg["world_bank"]["indicators"].keys()), cfg["world_bank"]["countries"]),
    )

    imf = ImfClient()
    write("imf", imf.get_many(list(cfg["imf"]["indicators"].keys()), cfg["world_bank"]["countries"]))

    mkt = cfg["market"]
    tickers = sum(mkt["tickers"].values(), [])
    write("market", MarketClient().get_prices(tickers, start=mkt.get("start", args.start)))

    ed = EdgarClient()
    if ed.enabled:
        write("sec_edgar", ed.get_many(cfg["sec_edgar"]["tickers"], cfg["sec_edgar"]["concepts"]))
    else:
        print("[sec_edgar] skipped — set SEC_USER_AGENT in .env")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())