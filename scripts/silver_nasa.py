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
    download_object,
    ensure_bucket,
    s3_client,
    setup_logging,
    upload_directory,
    write_clean_nasa_csv,
)

NASA_KEY = "nasa/GLB.Ts+dSST.csv"


def build_silver_df(spark, clean_csv: Path):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(clean_csv))
    )
    month_cols = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    stack_args = ", ".join([f"'{i:02d}', cast(`{m}` as string)" for i, m in enumerate(month_cols, 1)])
    df = df.select(F.col("Year").cast("int").alias("year"), F.expr(f"stack(12, {stack_args}) as (month, anomaly)"))
    df = df.withColumn(
        "anomaly",
        F.when(F.col("anomaly") == "***", F.lit(None)).otherwise(F.col("anomaly").cast("double")),
    )
    df = df.withColumn(
        "date",
        F.to_date(F.concat_ws("-", F.col("year"), F.col("month"), F.lit("01"))),
    )
    return df.select("year", "month", "date", "anomaly").orderBy("date")


def main() -> None:
    logger = setup_logging("silver_nasa")
    s3 = s3_client()
    ensure_bucket(s3, SILVER_BUCKET, logger)

    tmp_dir = OUTPUT_ROOT / "tmp" / "bronze" / "nasa"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    raw_csv = tmp_dir / "GLB.Ts+dSST.csv"
    download_object(s3, BRONZE_BUCKET, NASA_KEY, raw_csv)

    clean_csv = tmp_dir / "GLB.Ts+dSST.cleaned.csv"
    write_clean_nasa_csv(raw_csv, clean_csv)

    spark = create_spark("silver-nasa-gistemp")
    df = build_silver_df(spark, clean_csv)

    local_out = OUTPUT_ROOT / "silver" / "nasa_gistemp"
    if local_out.exists():
        shutil.rmtree(local_out)
    df.write.mode("overwrite").parquet(str(local_out))

    uploaded = upload_directory(s3, local_out, SILVER_BUCKET, "nasa/gistemp")
    logger.info("Silver NASA GISTEMP written to %s and uploaded (%s files).", local_out, uploaded)


if __name__ == "__main__":
    main()
