#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import functions as F

from common import (
    BRONZE_BUCKET,
    OUTPUT_ROOT,
    SILVER_BUCKET,
    create_spark,
    create_spark_df_from_pandas,
    download_object,
    ensure_bucket,
    extract_zip,
    list_keys,
    s3_client,
    summarize_era5_nc,
    setup_logging,
    upload_directory,
)

ERA5_PREFIX = "copernicus/era5_asean_2023_2024"
ERA5_ZIP_KEY = "copernicus/era5_asean_2023_2024.zip"


def build_era5_records(nc_files: list[Path]) -> list[dict]:
    records: list[dict] = []
    for nc_path in nc_files:
        records.extend(summarize_era5_nc(nc_path))
    return records


def main() -> None:
    logger = setup_logging("silver_era5")
    s3 = s3_client()
    ensure_bucket(s3, SILVER_BUCKET, logger)

    tmp_dir = OUTPUT_ROOT / "tmp" / "bronze" / "era5"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    nc_keys = [key for key in list_keys(s3, BRONZE_BUCKET, f"{ERA5_PREFIX}/") if key.endswith(".nc")]
    nc_files: list[Path] = []
    if nc_keys:
        for key in nc_keys:
            dest = tmp_dir / Path(key).name
            download_object(s3, BRONZE_BUCKET, key, dest)
            nc_files.append(dest)
    else:
        zip_path = tmp_dir / "era5_asean_2023_2024.zip"
        download_object(s3, BRONZE_BUCKET, ERA5_ZIP_KEY, zip_path)
        nc_files = extract_zip(zip_path, tmp_dir / "extracted")

    if not nc_files:
        raise FileNotFoundError("No ERA5 NetCDF files found in bronze bucket.")

    records = build_era5_records(nc_files)
    import pandas as pd

    pdf = pd.DataFrame(records)
    spark = create_spark("silver-era5")
    df = create_spark_df_from_pandas(spark, pdf)
    df = df.withColumn("time", F.col("time").cast("timestamp"))
    df = df.withColumn("value", F.col("value").cast("double"))
    df = df.select("time", "variable", "value", "units", "source_file").orderBy("time", "variable")

    local_out = OUTPUT_ROOT / "silver" / "era5"
    if local_out.exists():
        shutil.rmtree(local_out)
    df.write.mode("overwrite").parquet(str(local_out))

    uploaded = upload_directory(s3, local_out, SILVER_BUCKET, "copernicus/era5_asean")
    logger.info("Silver ERA5 written to %s and uploaded (%s files).", local_out, uploaded)


if __name__ == "__main__":
    main()
