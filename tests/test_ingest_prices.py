from __future__ import annotations
from pathlib import Path
import subprocess


BRONZE = Path("bronze/equities/tiingo/prices_eod")


def test_bronze_partition_written(tmp_path, monkeypatch):
# run a very small window to keep it fast
    cmd = [
        "python", "-m", "ingest.tiingo_prices_eod",
        "--from", "2025-09-22", "--to", "2025-09-22",
        "--tickers", "AAPL"
    ]
    subprocess.run(cmd, check=True)


# Find any date partition
partitions = list(BRONZE.glob("date=*/part-AAPL.parquet"))
assert partitions, "Expected at least one partition parquet file"