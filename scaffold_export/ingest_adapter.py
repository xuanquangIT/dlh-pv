"""Copied ingestion adapter for scaffold export.

This file mirrors the `scaffold/ingest_adapter.py` implementation so the
exported folder is self-contained.
"""
from pathlib import Path
import shutil
from typing import Optional

try:
    import boto3
except Exception:
    boto3 = None


class LocalAdapter:
    def __init__(self, base_path: str | Path = "data/raw"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def cache_file(self, src_path: str | Path, dest_name: Optional[str] = None) -> Path:
        src = Path(src_path)
        dest_name = dest_name or src.name
        dest = self.base_path / dest_name
        if src.resolve() == dest.resolve():
            return dest
        shutil.copy(src, dest)
        return dest

    def path(self, name: str) -> Path:
        return self.base_path / name


class S3Adapter:
    def __init__(self, bucket: str, prefix: str = "", cache_dir: str | Path = "data/raw"):
        if boto3 is None:
            raise RuntimeError("boto3 is required for S3Adapter")
        self.bucket = bucket
        self.prefix = prefix
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.s3 = boto3.client("s3")

    def download(self, key: str, dest_name: Optional[str] = None) -> Path:
        dest_name = dest_name or Path(key).name
        dest = self.cache_dir / dest_name
        self.s3.download_file(self.bucket, f"{self.prefix.rstrip('/')}/{key.lstrip('/')}", str(dest))
        return dest

    def list_keys(self, prefix: str = "") -> list:
        resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix or self.prefix)
        keys = [o["Key"] for o in resp.get("Contents", [])]
        return keys
