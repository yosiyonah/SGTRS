from __future__ import annotations
import os
from pathlib import Path
import pandas as pd


DATA_ROOT = Path(os.getenv("DATA_ROOT", "."))


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    df.to_parquet(path, index=False)


def bronze_partition_path(domain: str, source: str, table: str, date: str, part: str) -> Path:
    return DATA_ROOT / "bronze" / domain / source / table / f"date={date}" / f"part-{part}.parquet"