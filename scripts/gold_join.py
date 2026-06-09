#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pyspark.sql import functions as F

from common import (
    GOLD_BUCKET,
    OUTPUT_ROOT,
    SILVER_BUCKET,
    choose_match,
    create_spark,
    download_prefix,
    ensure_bucket,
    s3_client,
    setup_logging,
    upload_directory,
)

NASA_PREFIX = "nasa/gistemp"
ERA5_PREFIX = "copernicus/era5_asean"
GCB_PREFIX = "gcb/global_carbon_budget"


def main() -> None:
    logger = setup_logging("gold_join")
    s3 = s3_client()
    ensure_bucket(s3, GOLD_BUCKET, logger)

    nasa_dir = OUTPUT_ROOT / "tmp" / "silver" / "nasa_gistemp"
    era5_dir = OUTPUT_ROOT / "tmp" / "silver" / "era5"
    gcb_dir = OUTPUT_ROOT / "tmp" / "silver" / "gcb"
    download_prefix(s3, SILVER_BUCKET, NASA_PREFIX, nasa_dir)
    download_prefix(s3, SILVER_BUCKET, ERA5_PREFIX, era5_dir)
    download_prefix(s3, SILVER_BUCKET, GCB_PREFIX, gcb_dir)

    spark = create_spark("gold-multisource-join")
    nasa = spark.read.parquet(str(nasa_dir))
    era5 = spark.read.parquet(str(era5_dir))
    gcb = spark.read.parquet(str(gcb_dir))

    nasa_annual = (
        nasa.groupBy("year")
        .agg(F.avg("anomaly").alias("nasa_anomaly"))
        .orderBy("year")
    )

    era5_vars = [row["variable"] for row in era5.select("variable").distinct().collect()]
    era5_target = choose_match(era5_vars, ["t2m", "temp", "temperature", "tas"])
    if not era5_target:
        raise ValueError("ERA5 dataset has no variables to join.")

    era5_annual = (
        era5.filter(F.col("variable") == era5_target)
        .withColumn("year", F.year("time"))
        .groupBy("year")
        .agg(F.avg("value").alias("era5_value"))
        .orderBy("year")
    )

    gcb_metrics = [row["metric"] for row in gcb.select("metric").distinct().collect()]
    gcb_target = choose_match(gcb_metrics, ["fossil", "co2", "emission", "total"])
    if not gcb_target:
        raise ValueError("GCB dataset has no metrics to join.")

    gcb_annual = (
        gcb.filter(F.col("metric") == gcb_target)
        .groupBy("year")
        .agg(F.avg("value").alias("gcb_value"))
        .orderBy("year")
    )

    joined = (
        nasa_annual.join(era5_annual, "year", "left")
        .join(gcb_annual, "year", "left")
        .withColumn("era5_variable", F.lit(era5_target))
        .withColumn("gcb_metric", F.lit(gcb_target))
        .orderBy("year")
    )

    local_out = OUTPUT_ROOT / "gold" / "multi_source_join"
    if local_out.exists():
        shutil.rmtree(local_out)
    joined.write.mode("overwrite").parquet(str(local_out))

    uploaded = upload_directory(s3, local_out, GOLD_BUCKET, "climate/multi_source_join")
    logger.info("Gold multisource join written to %s and uploaded (%s files).", local_out, uploaded)


if __name__ == "__main__":
    main()
