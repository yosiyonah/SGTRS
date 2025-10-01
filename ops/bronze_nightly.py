"""
A tiny orchestrator: reads a seed list and runs the Tiingo ingestor for yesterday.
Use as a simple daily cron job before you adopt a full scheduler.
"""
from __future__ import annotations
import subprocess
from datetime import date, timedelta
from pathlib import Path


SEED = Path("config/tiingo_universe_seed.txt")


def run():
    y = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    tickers = ",".join([line.strip() for line in SEED.read_text().splitlines() if line.strip()])
    cmd = [
        "python", "-m", "ingest.tiingo_prices_eod",
        "--from", y, "--to", y,
        "--tickers", tickers,
    ]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    run()