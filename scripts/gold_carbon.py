#!/usr/bin/env python3
from __future__ import annotations

import shutil

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

GCB_PREFIX = "gcb/global_carbon_budget"


def main() -> None:
    logger = setup_logging("gold_carbon")
    s3 = s3_client()
    ensure_bucket(s3, GOLD_BUCKET, logger)

    gcb_dir = OUTPUT_ROOT / "tmp" / "silver" / "gcb"
    download_prefix(s3, SILVER_BUCKET, GCB_PREFIX, gcb_dir)

    spark = create_spark("gold-gcb-annual")
    gcb = spark.read.parquet(str(gcb_dir))

    gcb_annual = (
        gcb.groupBy("year", "metric")
        .agg(F.avg("value").alias("value"))
        .orderBy("year", "metric")
    )

    local_out = OUTPUT_ROOT / "gold" / "gcb_annual"
    if local_out.exists():
        shutil.rmtree(local_out)
    gcb_annual.write.mode("overwrite").parquet(str(local_out))

    uploaded = upload_directory(s3, local_out, GOLD_BUCKET, "climate/gcb_annual")
    logger.info("Gold GCB annual written to %s and uploaded (%s files).", local_out, uploaded)


if __name__ == "__main__":
    main()
