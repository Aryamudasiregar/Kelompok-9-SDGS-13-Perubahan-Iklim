#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

from botocore.config import Config

from common import BRONZE_BUCKET, OUTPUT_ROOT, s3_client, setup_logging


def count_local_files(path: Path) -> int:
    return sum(1 for _ in path.rglob("*") if _.is_file())


def count_s3_objects(prefix: str) -> int:
    s3 = s3_client()
    count = 0
    token = None
    while True:
        params = {"Bucket": BRONZE_BUCKET, "Prefix": prefix}
        if token:
            params["ContinuationToken"] = token
        resp = s3.list_objects_v2(**params)
        count += len(resp.get("Contents", []))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return count


def main() -> None:
    logger = setup_logging("check")

    local_dir = OUTPUT_ROOT / "bronze" / "noaa_ghcnd_all"
    if not local_dir.exists():
        raise FileNotFoundError(f"Missing local extract: {local_dir}")

    local_count = count_local_files(local_dir)
    s3_count = count_s3_objects("noaa/ghcnd_all/")

    logger.info("Local NOAA files: %s", local_count)
    logger.info("MinIO objects (bronze/noaa/ghcnd_all): %s", s3_count)
    if local_count != s3_count:
        logger.warning("Mismatch detected. Consider re-running scripts/bronze.py.")


if __name__ == "__main__":
    main()
