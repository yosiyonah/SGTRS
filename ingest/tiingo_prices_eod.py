from __future__ import annotations
import argparse, os
from datetime import date, timedelta
import requests
import pandas as pd
from dotenv import load_dotenv


from common.io import bronze_partition_path, write_parquet
from common.time_utils import utc_now_iso
from common.lineage import make_run_id


TIINGO_API = "https://api.tiingo.com/tiingo/daily/{ticker}/prices"




def daterange(d1: date, d2: date):
    # inclusive range [d1, d2]
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)




def fetch_tiingo_eod(ticker: str, start: str, end: str, token: str) -> pd.DataFrame:
    url = TIINGO_API.format(ticker=ticker)
    params = {"startDate": start, "endDate": end, "token": token}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    # Tiingo fields include: date, open, high, low, close, adjClose, volume, etc.
    df["ticker"] = ticker
    return df




def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fetch Tiingo EOD prices into Bronze partitions")
    parser.add_argument("--from", dest="from_", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="to", required=True, help="YYYY-MM-DD")
    parser.add_argument("--tickers", required=True, help="Comma-separated tickers")
    args = parser.parse_args()


token = os.getenv("TIINGO_API_KEY")
if not token:
    raise SystemExit("Missing TIINGO_API_KEY in .env")


run_id = make_run_id()
tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]


# Fetch once per ticker for the whole window, then write one part per date
for tkr in tickers:
    df = fetch_tiingo_eod(tkr, args.from_, args.to, token)
    if df.empty:
        print(f"No data for {tkr} in range {args.from_}..{args.to}")
        continue
    # Normalize date to YYYY-MM-DD
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["source"] = "tiingo"
    df["ingest_ts_utc"] = utc_now_iso()
    df["ingest_run_id"] = run_id


    for d in sorted(df["date"].unique()):
        part_df = df[df["date"] == d].copy()
        path = bronze_partition_path("equities", "tiingo", "prices_eod", d, part=tkr)
        write_parquet(part_df, path)
        print(f"Wrote {len(part_df)} rows -> {path}")


if __name__ == "__main__":
    main()