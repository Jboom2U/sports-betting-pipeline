"""
db/csv_sync.py
Syncs CSV data files to/from S3-compatible object storage (Cloudflare R2 or Railway).

WHY THIS EXISTS:
  Every `railway up` destroys data/raw/ and data/clean/. This module:
    - Downloads all stored CSVs on startup so the model has data immediately
    - Uploads updated CSVs after the pipeline completes so data persists across deploys

STORAGE SETUP (Cloudflare R2 or Railway object storage — both are S3-compatible):
  Set these env vars in Railway:
    STORAGE_ENDPOINT_URL      — R2: https://<account_id>.r2.cloudflarestorage.com
                                 Railway: provided when you add Object Storage
    STORAGE_ACCESS_KEY_ID     — R2 API token Access Key ID
    STORAGE_SECRET_ACCESS_KEY — R2 API token Secret Access Key
    STORAGE_BUCKET            — bucket name, e.g. "statalizers-data"

  If any of these are absent, all sync functions are no-ops (non-fatal).

PUBLIC API:
    download_all()    -> int   — pull all CSVs from storage to local disk on startup
    upload_all()      -> int   — push all CSVs from local disk to storage after pipeline
    upload_file(path) -> bool  — upload a single file (used for incremental updates)
    storage_available() -> bool
"""

import os
import logging
from pathlib import Path

log = logging.getLogger(__name__)

BASE_DIR  = Path(__file__).parent.parent
CLEAN_DIR = BASE_DIR / "data" / "clean"
RAW_DIR   = BASE_DIR / "data" / "raw"

# File patterns synced to storage. Raw files are large and transient;
# clean/ masters are the critical ones — they're what the model reads.
# We sync both to be safe.
SYNC_PATTERNS = [
    (CLEAN_DIR, "clean/", ["*.csv"]),          # all clean master CSVs
    (RAW_DIR,   "raw/",   ["mlb_weather_*.csv",
                            "mlb_line_movement_*.csv",
                            "mlb_odds_master.csv"]),  # smaller raw files worth keeping
]


# ── Boto3 client ──────────────────────────────────────────────────────────────

_s3_client = None


def _get_client():
    """Build and cache a boto3 S3 client configured for the object store."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    endpoint  = os.environ.get("STORAGE_ENDPOINT_URL", "").strip()
    access_key = os.environ.get("STORAGE_ACCESS_KEY_ID", "").strip()
    secret_key = os.environ.get("STORAGE_SECRET_ACCESS_KEY", "").strip()

    if not all([endpoint, access_key, secret_key]):
        log.debug("STORAGE_* env vars not set — CSV sync disabled.")
        return None

    try:
        import boto3
        from botocore.config import Config

        _s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
            ),
            region_name="auto",   # R2 requires this
        )
        log.info("Object storage client initialized.")
        return _s3_client

    except ImportError:
        log.warning("boto3 not installed — CSV sync disabled. Run: pip install boto3")
        return None
    except Exception as e:
        log.warning(f"Object storage client setup failed (non-fatal): {e}")
        return None


def _bucket() -> str:
    return os.environ.get("STORAGE_BUCKET", "statalizers-data").strip()


def storage_available() -> bool:
    """Quick check — returns True if object storage is configured."""
    return _get_client() is not None


# ── Download ──────────────────────────────────────────────────────────────────

def download_all() -> int:
    """
    Download all stored CSVs from object storage to local disk.
    Called on startup so the model has data even after a fresh deploy.
    Returns number of files downloaded.
    """
    client = _get_client()
    if client is None:
        return 0

    bucket   = _bucket()
    downloaded = 0

    try:
        # List all objects in the bucket
        paginator = client.get_paginator("list_objects_v2")
        pages     = paginator.paginate(Bucket=bucket)

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]   # e.g. "clean/mlb_scores_master.csv"

                # Map storage key back to local path
                local_path = _key_to_local(key)
                if local_path is None:
                    continue

                # Skip if local file is newer than what's in storage
                # (avoids overwriting a freshly-run pipeline with stale data)
                if local_path.exists():
                    local_mtime  = local_path.stat().st_mtime
                    storage_mtime = obj["LastModified"].timestamp()
                    if local_mtime >= storage_mtime:
                        log.debug(f"Skip download (local newer): {key}")
                        continue

                local_path.parent.mkdir(parents=True, exist_ok=True)
                client.download_file(bucket, key, str(local_path))
                log.info(f"Downloaded: {key} → {local_path.name}")
                downloaded += 1

        log.info(f"CSV sync download complete: {downloaded} file(s) pulled from storage.")
        return downloaded

    except Exception as e:
        log.warning(f"download_all failed (non-fatal): {e}")
        return downloaded


def _key_to_local(key: str) -> Path | None:
    """Convert a storage key like 'clean/foo.csv' to an absolute local path."""
    if key.startswith("clean/"):
        return CLEAN_DIR / key[len("clean/"):]
    if key.startswith("raw/"):
        return RAW_DIR / key[len("raw/"):]
    return None


# ── Upload ────────────────────────────────────────────────────────────────────

def upload_all() -> int:
    """
    Upload all local CSVs to object storage after the pipeline completes.
    Returns number of files uploaded.
    """
    client = _get_client()
    if client is None:
        return 0

    bucket   = _bucket()
    uploaded = 0

    for local_dir, prefix, patterns in SYNC_PATTERNS:
        local_dir = Path(local_dir)
        if not local_dir.exists():
            continue

        import glob as _glob
        for pattern in patterns:
            for filepath in local_dir.glob(pattern):
                key = prefix + filepath.name
                try:
                    client.upload_file(str(filepath), bucket, key)
                    log.info(f"Uploaded: {filepath.name} → {key}")
                    uploaded += 1
                except Exception as e:
                    log.warning(f"Upload failed for {filepath.name} (non-fatal): {e}")

    log.info(f"CSV sync upload complete: {uploaded} file(s) pushed to storage.")
    return uploaded


def upload_file(local_path: str | Path, storage_key: str = None) -> bool:
    """
    Upload a single file to object storage.
    storage_key defaults to 'clean/<filename>' or 'raw/<filename>' based on path.
    """
    client = _get_client()
    if client is None:
        return False

    local_path = Path(local_path)
    if not local_path.exists():
        log.warning(f"upload_file: {local_path} not found.")
        return False

    if storage_key is None:
        if "clean" in str(local_path):
            storage_key = f"clean/{local_path.name}"
        else:
            storage_key = f"raw/{local_path.name}"

    try:
        client.upload_file(str(local_path), _bucket(), storage_key)
        log.info(f"Uploaded: {local_path.name} → {storage_key}")
        return True
    except Exception as e:
        log.warning(f"upload_file failed for {local_path.name} (non-fatal): {e}")
        return False
