#!/usr/bin/env python3
from __future__ import annotations

import shutil

from pyspark.sql import functions as F

from common import (
    BRONZE_BUCKET,
    OUTPUT_ROOT,
    SILVER_BUCKET,
    create_spark,
    create_spark_df_from_pandas,
    download_object,
    ensure_bucket,
    load_gcb_long_df,
    s3_client,
    setup_logging,
    upload_directory,
)

GCB_KEY = "gcb/Global_Carbon_Budget_2024_v1.0-3.xlsx"


def build_silver_df(spark, xlsx_path):
    pdf = load_gcb_long_df(xlsx_path)
    df = create_spark_df_from_pandas(spark, pdf)
    df = df.withColumn("year", F.col("year").cast("int"))
    df = df.withColumn("value", F.col("value").cast("double"))
    return df.select("year", "metric", "value").orderBy("year", "metric")


def main() -> None:
    logger = setup_logging("silver_gcb")
    s3 = s3_client()
    ensure_bucket(s3, SILVER_BUCKET, logger)

    tmp_dir = OUTPUT_ROOT / "tmp" / "bronze" / "gcb"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    raw_xlsx = tmp_dir / "Global_Carbon_Budget_2024_v1.0-3.xlsx"
    download_object(s3, BRONZE_BUCKET, GCB_KEY, raw_xlsx)

    spark = create_spark("silver-gcb")
    df = build_silver_df(spark, raw_xlsx)

    local_out = OUTPUT_ROOT / "silver" / "gcb"
    if local_out.exists():
        shutil.rmtree(local_out)
    df.write.mode("overwrite").parquet(str(local_out))

    uploaded = upload_directory(s3, local_out, SILVER_BUCKET, "gcb/global_carbon_budget")
    logger.info("Silver GCB written to %s and uploaded (%s files).", local_out, uploaded)


if __name__ == "__main__":
    main()
