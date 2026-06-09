#!/usr/bin/env python3
from __future__ import annotations

import tarfile
from pathlib import Path

from common import (
    BRONZE_BUCKET,
    DATA_ROOT,
    OUTPUT_ROOT,
    TRANSFER_CONFIG,
    ensure_bucket,
    s3_client,
    setup_logging,
)


def human_size(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024:
            return f"{num:.1f}{unit}"
        num /= 1024
    return f"{num:.1f}PB"


def upload_file(s3, local_path: Path, bucket: str, key: str, logger) -> None:
    size = local_path.stat().st_size
    logger.info("Uploading %s -> s3://%s/%s (%s)", local_path, bucket, key, human_size(size))
    s3.upload_file(str(local_path), bucket, key, Config=TRANSFER_CONFIG)


def upload_directory(s3, base_dir: Path, bucket: str, prefix: str, logger) -> int:
    base_dir = base_dir.resolve()
    files = [path for path in base_dir.rglob("*") if path.is_file()]
    for path in files:
        rel = path.relative_to(base_dir).as_posix()
        key = f"{prefix}/{rel}" if prefix else rel
        upload_file(s3, path, bucket, key, logger)
    return len(files)


def safe_extract(tar: tarfile.TarFile, path: Path) -> None:
    base = path.resolve()
    for member in tar.getmembers():
        member_path = (base / member.name).resolve()
        if not member_path.is_relative_to(base):
            raise ValueError(f"Unsafe path in tar: {member.name}")
    tar.extractall(path=base)


def extract_noaa(tar_path: Path, extract_dir: Path, logger) -> Path:
    if extract_dir.exists():
        existing = [path for path in extract_dir.rglob("*") if path.is_file()]
        if existing:
            logger.info("NOAA already extracted: %s (%s files)", extract_dir, len(existing))
            return extract_dir
    extract_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Extracting %s -> %s", tar_path, extract_dir)
    with tarfile.open(tar_path, "r:gz") as tar:
        safe_extract(tar, extract_dir)
    return extract_dir


def upload_era5(s3, logger) -> int:
    era5_dir = DATA_ROOT / "copernicus" / "era5_asean_2023_2024"
    era5_zip = DATA_ROOT / "copernicus" / "era5_asean_2023_2024.zip"
    nc_files = list(era5_dir.rglob("*.nc")) if era5_dir.exists() else []
    if nc_files:
        logger.info("Found %s NetCDF files in %s", len(nc_files), era5_dir)
        return upload_directory(
            s3, era5_dir, BRONZE_BUCKET, "copernicus/era5_asean_2023_2024", logger
        )
    if era5_zip.exists():
        upload_file(s3, era5_zip, BRONZE_BUCKET, "copernicus/era5_asean_2023_2024.zip", logger)
        return 1
    raise FileNotFoundError("ERA5 dataset not found (no .nc files or zip).")


def main() -> None:
    logger = setup_logging("bronze")
    s3 = s3_client()
    ensure_bucket(s3, BRONZE_BUCKET, logger)

    nasa_file = DATA_ROOT / "nasa" / "GLB.Ts+dSST.csv"
    gcb_file = DATA_ROOT / "gcb" / "Global_Carbon_Budget_2024_v1.0-3.xlsx"
    noaa_dir = DATA_ROOT / "noaa"
    noaa_tar = noaa_dir / "ghcnd_all.tar.gz"
    noaa_stations = noaa_dir / "ghcnd-stations.txt"
    noaa_inventory = noaa_dir / "ghcnd-inventory.txt"

    for path in (nasa_file, gcb_file, noaa_stations, noaa_inventory):
        if not path.exists():
            raise FileNotFoundError(f"Missing required file: {path}")

    upload_file(s3, nasa_file, BRONZE_BUCKET, "nasa/GLB.Ts+dSST.csv", logger)
    upload_file(s3, gcb_file, BRONZE_BUCKET, "gcb/Global_Carbon_Budget_2024_v1.0-3.xlsx", logger)
    upload_era5(s3, logger)
    upload_file(s3, noaa_stations, BRONZE_BUCKET, "noaa/ghcnd-stations.txt", logger)
    upload_file(s3, noaa_inventory, BRONZE_BUCKET, "noaa/ghcnd-inventory.txt", logger)

    if not noaa_tar.exists():
        raise FileNotFoundError(f"Missing NOAA archive: {noaa_tar}")

    extract_dir = OUTPUT_ROOT / "bronze" / "noaa_ghcnd_all"
    extract_dir = extract_noaa(noaa_tar, extract_dir, logger)
    count = upload_directory(s3, extract_dir, BRONZE_BUCKET, "noaa/ghcnd_all", logger)
    logger.info("Uploaded %s NOAA daily files to bronze.", count)


if __name__ == "__main__":
    main()
