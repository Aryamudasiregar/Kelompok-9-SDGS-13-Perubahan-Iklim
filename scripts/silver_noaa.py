#!/usr/bin/env python3
from __future__ import annotations

import shutil
from pathlib import Path

from botocore.exceptions import ClientError
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from common import (
    BRONZE_BUCKET,
    NOAA_ELEMENTS,
    OUTPUT_ROOT,
    SILVER_BUCKET,
    create_spark,
    create_spark_df_from_pandas,
    download_object,
    ensure_bucket,
    load_noaa_bounds,
    load_noaa_inventory,
    load_noaa_stations,
    parse_noaa_dly_lines,
    s3_client,
    setup_logging,
    upload_directory,
)

STATIONS_KEY = "noaa/ghcnd-stations.txt"
INVENTORY_KEY = "noaa/ghcnd-inventory.txt"
NOAA_DAILY_PREFIXES = ("noaa/ghcnd_all", "noaa/ghcnd_all/ghcnd_all")

def parse_dly_file(path: Path, station_meta: dict) -> list[dict]:
    with path.open("r", encoding="utf-8") as f:
        return parse_noaa_dly_lines(f, station_meta, NOAA_ELEMENTS)


def build_silver_df(records: list[dict], spark):
    import pandas as pd

    pdf = pd.DataFrame(records)
    schema = StructType(
        [
            StructField("station_id", StringType(), False),
            StructField("date", StringType(), False),
            StructField("element", StringType(), False),
            StructField("value", DoubleType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("elevation", DoubleType(), True),
            StructField("name", StringType(), True),
            StructField("mflag", StringType(), True),
            StructField("qflag", StringType(), True),
            StructField("sflag", StringType(), True),
        ]
    )
    df = spark.createDataFrame(pdf.astype(object).where(pd.notna(pdf), None).to_dict("records"), schema=schema)
    df = df.withColumn("date", F.to_date("date")).withColumn("value", F.col("value").cast("double"))
    return df.select(
        "station_id",
        "date",
        "element",
        "value",
        "lat",
        "lon",
        "elevation",
        "name",
        "mflag",
        "qflag",
        "sflag",
    )


def main() -> None:
    logger = setup_logging("silver_noaa")
    s3 = s3_client()
    ensure_bucket(s3, SILVER_BUCKET, logger)

    tmp_dir = OUTPUT_ROOT / "tmp" / "bronze" / "noaa"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    stations_path = tmp_dir / "ghcnd-stations.txt"
    inventory_path = tmp_dir / "ghcnd-inventory.txt"

    download_object(s3, BRONZE_BUCKET, STATIONS_KEY, stations_path)
    download_object(s3, BRONZE_BUCKET, INVENTORY_KEY, inventory_path)

    lat_min, lat_max, lon_min, lon_max, max_stations = load_noaa_bounds()
    stations = load_noaa_stations(stations_path, lat_min, lat_max, lon_min, lon_max)
    eligible = load_noaa_inventory(inventory_path, NOAA_ELEMENTS)
    stations = [s for s in stations if s["station_id"] in eligible][:max_stations]
    if not stations:
        raise ValueError("No stations matched the bounding box filter.")

    station_map = {s["station_id"]: s for s in stations}
    daily_dir = tmp_dir / "ghcnd_all"
    daily_dir.mkdir(parents=True, exist_ok=True)

    downloaded = []
    missing = []
    for station_id in station_map:
        dest = daily_dir / f"{station_id}.dly"
        for prefix in NOAA_DAILY_PREFIXES:
            key = f"{prefix}/{station_id}.dly"
            try:
                download_object(s3, BRONZE_BUCKET, key, dest)
                downloaded.append(dest)
                break
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code not in ("404", "NoSuchKey"):
                    raise
        else:
            missing.append(station_id)

    if not downloaded:
        raise FileNotFoundError("No NOAA daily files downloaded for selected stations.")
    if missing:
        logger.warning("Missing %s station files (skipped).", len(missing))

    records: list[dict] = []
    for path in downloaded:
        station_id = path.stem
        records.extend(parse_dly_file(path, station_map[station_id]))

    spark = create_spark("silver-noaa")
    df = build_silver_df(records, spark)

    local_out = OUTPUT_ROOT / "silver" / "noaa_daily"
    if local_out.exists():
        shutil.rmtree(local_out)
    df.write.mode("overwrite").parquet(str(local_out))

    uploaded = upload_directory(s3, local_out, SILVER_BUCKET, "noaa/ghcnd_daily")
    logger.info("Silver NOAA daily written to %s and uploaded (%s files).", local_out, uploaded)


if __name__ == "__main__":
    main()
