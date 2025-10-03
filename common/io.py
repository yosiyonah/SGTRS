from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Dict, Union

import pandas as pd


# IMPORTANT: load .env at import time so env vars are visible even if callers load later
try:
    from dotenv import load_dotenv, find_dotenv # type: ignore
    load_dotenv(find_dotenv(), override=False)
except Exception:
    pass


DATA_ROOT = Path(os.getenv("DATA_ROOT", "."))




def _protocol() -> str:
    return os.getenv("DATA_PROTOCOL", "local").lower()




def _s3_storage_opts() -> Dict[str, Any]:
    opts: Dict[str, Any] = {}

    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")
    if access_key and secret_key:
        opts["key"] = access_key
        opts["secret"] = secret_key

    client_kwargs: Dict[str, Any] = {}
    endpoint = os.getenv("S3_ENDPOINT_URL")
    region = os.getenv("AWS_REGION") or os.getenv("S3_REGION")
    if endpoint:
        client_kwargs["endpoint_url"] = endpoint
    if region:
        client_kwargs["region_name"] = region
    if client_kwargs:
        opts["client_kwargs"] = client_kwargs

    return opts




def bronze_partition_path(domain: str, source: str, table: str, date: str, part: str) -> Union[str, Path]:
    if _protocol() == "s3":
        bucket = os.getenv("S3_BUCKET")
        if not bucket:
            raise RuntimeError("DATA_PROTOCOL is 's3' but S3_BUCKET is not configured")
        bucket = bucket.rstrip("/")
        key = f"bronze/{domain}/{source}/{table}/date={date}/part-{part}.parquet"
        return f"s3://{bucket}/{key}"

    return DATA_ROOT / "bronze" / domain / source / table / f"date={date}" / f"part-{part}.parquet"




def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)




def write_parquet(df: pd.DataFrame, path: Union[str, Path]) -> None:
    """Write parquet to local disk or S3 depending on path type."""
    if isinstance(path, str) and path.startswith("s3://"):
        df.to_parquet(path, index=False, storage_options=_s3_storage_opts())
    else:
        ensure_dir(Path(path).parent)
        df.to_parquet(path, index=False)
