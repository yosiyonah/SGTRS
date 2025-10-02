from __future__ import annotations
import argparse, os
from datetime import date, timedelta
import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv


from common.io import bronze_partition_path, write_parquet
from common.time_utils import utc_now_iso
from common.lineage import make_run_id


TIINGO_API = "https://api.tiingo.com/tiingo/daily/{ticker}/prices"



def fetch_tiingo_eod(ticker: str, start: str, end: str, token: str) -> pd.DataFrame:
    url = TIINGO_API.format(ticker=ticker)
    params = {"startDate": start, "endDate": end, "token": token}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df["ticker"] = ticker
    return df




def main():
    # Locate .env even if cwd isn't repo root
    load_dotenv(find_dotenv())

    parser = argparse.ArgumentParser(
        description="Fetch Tiingo EOD prices into Bronze partitions (one file per date)"
    )
    parser.add_argument("--from", dest="from_", required=True, help="YYYY-MM-DD")
    parser.add_argument("--to", dest="to", required=True, help="YYYY-MM-DD")
    parser.add_argument("--tickers", required=True, help="Comma-separated tickers")
    args = parser.parse_args()


    token = os.getenv("TIINGO_API_KEY")
    if not token:
        raise SystemExit("Missing TIINGO_API_KEY in .env")


    run_id = make_run_id()
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]


    # Fetch each ticker once for the full window, then write one parquet per date containing all rows
    frames: list[pd.DataFrame] = []
    for tkr in tickers:
        df = fetch_tiingo_eod(tkr, args.from_, args.to, token)
        if df.empty:
            print(f"No data for {tkr} in range {args.from_}..{args.to}")
            continue
        frames.append(df)


    if not frames:
        print("No data fetched for any ticker — nothing to write.")
        return


    big = pd.concat(frames, ignore_index=True)
    big["date"] = pd.to_datetime(big["date"]).dt.strftime("%Y-%m-%d")
    big["source"] = "tiingo"
    big["ingest_ts_utc"] = utc_now_iso()
    big["ingest_run_id"] = run_id


    # Write one file per date (cuts small-file count drastically)
    for d, g in big.groupby("date", sort=True):
        path = bronze_partition_path("equities", "tiingo", "prices_eod", d, part="0000")
        write_parquet(g, path)  # overwrites if exists
        print(f"Wrote {len(g)} rows -> {path}")


if __name__ == "__main__":
    main()
