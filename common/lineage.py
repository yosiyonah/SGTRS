from __future__ import annotations
import hashlib, os, time


def make_run_id() -> str:
    base = f"{time.time_ns()}-{os.getpid()}"
    return hashlib.sha256(base.encode()).hexdigest()[:12]


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()