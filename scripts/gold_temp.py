#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import functions as F

from common import (
    GOLD_BUCKET,
    OUTPUT_ROOT,
    SILVER_BUCKET,
    create_spark,
    download_prefix,
    ensure_bucket,
    s3_client,
    setup_logging,
    upload_directory,
)

NASA_PREFIX = "nasa/gistemp"
ERA5_PREFIX = "copernicus/era5_asean"


def main() -> None:
    logger = setup_logging("gold_temp")
    s3 = s3_client()
    ensure_bucket(s3, GOLD_BUCKET, logger)

    nasa_dir = OUTPUT_ROOT / "tmp" / "silver" / "nasa_gistemp"
    era5_dir = OUTPUT_ROOT / "tmp" / "silver" / "era5"
    download_prefix(s3, SILVER_BUCKET, NASA_PREFIX, nasa_dir)
    download_prefix(s3, SILVER_BUCKET, ERA5_PREFIX, era5_dir)

    spark = create_spark("gold-temp-trend")
    nasa = spark.read.parquet(str(nasa_dir))
    era5 = spark.read.parquet(str(era5_dir))

    nasa_trend = (
        nasa.select(
            F.col("date").alias("date"),
            F.lit("NASA_GISTEMP").alias("source"),
            F.lit("temp_anomaly").alias("variable"),
            F.lit("C").alias("unit"),
            F.col("anomaly").cast("double").alias("value"),
        )
        .where(F.col("value").isNotNull())
    )

    era5_trend = (
        era5.withColumn("date", F.date_trunc("month", F.col("time")))
        .groupBy("date", "variable", "units")
        .agg(F.avg("value").alias("value"))
        .select(
            F.col("date"),
            F.lit("ERA5").alias("source"),
            F.col("variable"),
            F.col("units").alias("unit"),
            F.col("value"),
        )
    )

    trend = nasa_trend.unionByName(era5_trend).orderBy("date", "source", "variable")

    local_out = OUTPUT_ROOT / "gold" / "temp_trend"
    if local_out.exists():
        shutil.rmtree(local_out)
    trend.write.mode("overwrite").parquet(str(local_out))

    uploaded = upload_directory(s3, local_out, GOLD_BUCKET, "climate/temp_trend")
    logger.info("Gold temp trend written to %s and uploaded (%s files).", local_out, uploaded)


if __name__ == "__main__":
    main()
