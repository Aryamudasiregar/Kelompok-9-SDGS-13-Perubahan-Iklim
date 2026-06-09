#!/usr/bin/env python3
from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Iterable

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data" / "raw"
OUTPUT_ROOT = PROJECT_ROOT / "output"

SPARK_LOCAL_CORES = os.getenv("SPARK_LOCAL_CORES", "1")
SPARK_MASTER = f"local[{SPARK_LOCAL_CORES}]"
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "1536m")
SPARK_SQL_SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "2")
SPARK_DEFAULT_PARALLELISM = os.getenv("SPARK_DEFAULT_PARALLELISM", "2")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")
GOLD_BUCKET = os.getenv("GOLD_BUCKET", "gold")

TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=64 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
    max_concurrency=4,
    use_threads=True,
)

NASA_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
NOAA_ELEMENTS = {"TMAX", "TMIN", "TAVG", "PRCP"}


def ensure_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_bucket(s3, bucket: str, logger: logging.Logger | None = None) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
        if logger:
            logger.info("Bucket exists: %s", bucket)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket)
            if logger:
                logger.info("Created bucket: %s", bucket)
        else:
            raise


def list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if token:
            params["ContinuationToken"] = token
        resp = s3.list_objects_v2(**params)
        for item in resp.get("Contents", []):
            keys.append(item["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def download_object(s3, bucket: str, key: str, dest_path: Path) -> Path:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(dest_path))
    return dest_path


def download_prefix(s3, bucket: str, prefix: str, dest_dir: Path, suffix: str | None = None) -> list[Path]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    keys = list_keys(s3, bucket, prefix)
    local_paths: list[Path] = []
    for key in keys:
        if key.endswith("/"):
            continue
        if suffix and not key.endswith(suffix):
            continue
        rel = key[len(prefix) :].lstrip("/")
        local_path = dest_dir / rel
        download_object(s3, bucket, key, local_path)
        local_paths.append(local_path)
    return local_paths


def upload_directory(s3, local_dir: Path, bucket: str, prefix: str) -> int:
    local_dir = local_dir.resolve()
    files = [path for path in local_dir.rglob("*") if path.is_file()]
    for path in files:
        rel = path.relative_to(local_dir).as_posix()
        key = f"{prefix}/{rel}" if prefix else rel
        s3.upload_file(str(path), bucket, key, Config=TRANSFER_CONFIG)
    return len(files)


def create_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
        .config("spark.default.parallelism", SPARK_DEFAULT_PARALLELISM)
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_spark_df_from_pandas(spark: SparkSession, pdf):
    import pandas as pd

    normalized = pdf.astype(object).where(pd.notna(pdf), None)
    return spark.createDataFrame(normalized.to_dict("records"))


def setup_logging(script_name: str, log_dir: Path | None = None) -> logging.Logger:
    log_dir = log_dir or (PROJECT_ROOT / "logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(script_name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_dir / f"{script_name}.log", encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def choose_match(candidates: Iterable[str], preferred: list[str]) -> str | None:
    candidates = [c for c in candidates if c]
    if not candidates:
        return None
    for pref in preferred:
        for cand in candidates:
            if pref in cand.lower():
                return cand
    return candidates[0]


def read_nasa_lines(raw_path: Path) -> list[str]:
    with raw_path.open("r", encoding="utf-8") as f:
        lines = [line for line in f.readlines() if line.strip()]
    if lines and lines[0].startswith("Land-Ocean"):
        lines = lines[1:]
    return lines


def write_clean_nasa_csv(raw_path: Path, clean_path: Path) -> Path:
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    lines = read_nasa_lines(raw_path)
    clean_path.write_text("".join(lines), encoding="utf-8")
    return clean_path


def load_nasa_raw_df(raw_path: Path):
    import io
    import pandas as pd

    lines = read_nasa_lines(raw_path)
    df = pd.read_csv(io.StringIO("".join(lines)))
    df.replace("***", pd.NA, inplace=True)
    return df


def load_nasa_long_df(raw_path: Path):
    import io
    import numpy as np
    import pandas as pd

    lines = read_nasa_lines(raw_path)
    df = pd.read_csv(io.StringIO("".join(lines)))
    df.replace("***", np.nan, inplace=True)
    df_long = df.melt(id_vars=["Year"], value_vars=NASA_MONTHS, var_name="month", value_name="anomaly")
    df_long["month"] = df_long["month"].apply(lambda m: NASA_MONTHS.index(m) + 1)
    df_long["year"] = df_long["Year"].astype(int)
    df_long["date"] = pd.to_datetime(
        df_long["year"].astype(str) + "-" + df_long["month"].astype(str).str.zfill(2) + "-01"
    )
    df_long["anomaly"] = pd.to_numeric(df_long["anomaly"], errors="coerce")
    return df_long[["year", "month", "date", "anomaly"]]


def to_snake(name: str) -> str:
    import re

    name = str(name).strip().replace("\n", " ")
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name.lower() or "col"


def pick_gcb_sheet(xlsx_path: Path) -> str:
    import pandas as pd

    xl = pd.ExcelFile(xlsx_path)
    for name in xl.sheet_names:
        if "global" in name.lower() and "budget" in name.lower():
            return name
    return xl.sheet_names[0]


def load_gcb_raw_df(xlsx_path: Path):
    import pandas as pd

    sheet = pick_gcb_sheet(xlsx_path)
    df = pd.read_excel(xlsx_path, sheet_name=sheet)
    df = df.dropna(axis=1, how="all")
    return df


def load_gcb_long_df(xlsx_path: Path):
    import pandas as pd

    pdf = load_gcb_raw_df(xlsx_path)
    pdf.columns = [to_snake(str(col)) for col in pdf.columns]
    year_col = next((c for c in pdf.columns if "year" in c), pdf.columns[0])
    pdf[year_col] = pd.to_numeric(pdf[year_col], errors="coerce")
    pdf = pdf[pdf[year_col].between(1800, 2100)]
    pdf[year_col] = pdf[year_col].astype(int)
    value_cols = [c for c in pdf.columns if c != year_col]
    long_pdf = pdf.melt(id_vars=[year_col], value_vars=value_cols, var_name="metric", value_name="value")
    long_pdf["value"] = pd.to_numeric(long_pdf["value"], errors="coerce")
    long_pdf = long_pdf.dropna(subset=["value"])
    long_pdf = long_pdf.rename(columns={year_col: "year"})
    return long_pdf[["year", "metric", "value"]]


def extract_zip(zip_path: Path, extract_dir: Path) -> list[Path]:
    import zipfile

    if extract_dir.exists():
        existing = list(extract_dir.rglob("*.nc"))
        if existing:
            return existing
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    return list(extract_dir.rglob("*.nc"))


def summarize_era5_nc(nc_path: Path) -> list[dict]:
    import cftime
    import numpy as np
    import pandas as pd
    from netCDF4 import Dataset, num2date

    records: list[dict] = []
    with Dataset(nc_path, "r") as ds:
        time_var = ds.variables.get("time")
        if time_var is None:
            for name, var in ds.variables.items():
                if getattr(var, "standard_name", None) == "time" or getattr(var, "long_name", None) == "time":
                    time_var = var
                    break
                if name.endswith("time") and len(var.dimensions) == 1 and var.dimensions[0] == name:
                    time_var = var
                    break
        if time_var is None:
            return records
        times = num2date(time_var[:], units=time_var.units)
        time_dim = time_var.dimensions[0] if time_var.dimensions else time_var.name
        coord_names = {"time", "valid_time", "latitude", "longitude", "lat", "lon", "number", "expver"}
        for name, var in ds.variables.items():
            if name in coord_names or time_dim not in var.dimensions:
                continue
            if len(var.dimensions) < 2:
                continue
            data = np.ma.filled(var[:], np.nan)
            time_axis = var.dimensions.index(time_dim)
            data = np.moveaxis(data, time_axis, 0)
            if data.ndim > 1:
                mean_vals = np.nanmean(data, axis=tuple(range(1, data.ndim)))
            else:
                mean_vals = data
            units = getattr(var, "units", None)
            for idx, tstamp in enumerate(times):
                if isinstance(tstamp, cftime.datetime):
                    tstamp = tstamp.strftime("%Y-%m-%d %H:%M:%S")
                time_value = pd.to_datetime(tstamp).strftime("%Y-%m-%d %H:%M:%S")
                records.append(
                    {
                        "time": time_value,
                        "variable": name,
                        "value": float(mean_vals[idx]) if idx < len(mean_vals) else None,
                        "units": units,
                        "source_file": nc_path.name,
                    }
                )
    return records


def load_noaa_bounds():
    lat_min = float(os.getenv("NOAA_LAT_MIN", "-11"))
    lat_max = float(os.getenv("NOAA_LAT_MAX", "28"))
    lon_min = float(os.getenv("NOAA_LON_MIN", "90"))
    lon_max = float(os.getenv("NOAA_LON_MAX", "142"))
    max_stations = int(os.getenv("NOAA_MAX_STATIONS", "200"))
    return lat_min, lat_max, lon_min, lon_max, max_stations


def parse_station_line(line: str) -> dict:
    return {
        "station_id": line[0:11].strip(),
        "lat": float(line[12:20].strip()),
        "lon": float(line[21:30].strip()),
        "elevation": float(line[31:37].strip() or 0.0),
        "name": line[41:71].strip(),
    }


def load_noaa_stations(station_path: Path, lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> list[dict]:
    stations: list[dict] = []
    with station_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            record = parse_station_line(line)
            if lat_min <= record["lat"] <= lat_max and lon_min <= record["lon"] <= lon_max:
                stations.append(record)
    return stations


def load_noaa_inventory(inventory_path: Path, elements: set[str] | None = None) -> set[str]:
    elements = elements or NOAA_ELEMENTS
    station_ids: set[str] = set()
    with inventory_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            station_id = line[0:11].strip()
            element = line[31:35].strip()
            if element in elements:
                station_ids.add(station_id)
    return station_ids


def parse_noaa_dly_lines(lines: Iterable[str], station_meta: dict, elements: set[str] | None = None) -> list[dict]:
    import datetime as dt

    elements = elements or NOAA_ELEMENTS
    records: list[dict] = []
    for line in lines:
        if len(line) < 21:
            continue
        station_id = line[0:11].strip()
        year = int(line[11:15])
        month = int(line[15:17])
        element = line[17:21].strip()
        if element not in elements:
            continue
        for day in range(1, 32):
            base = 21 + (day - 1) * 8
            value_raw = line[base : base + 5]
            qflag = line[base + 6 : base + 7]
            mflag = line[base + 5 : base + 6]
            sflag = line[base + 7 : base + 8]
            if not value_raw.strip():
                continue
            value = int(value_raw)
            if value == -9999 or qflag.strip():
                continue
            try:
                date = dt.date(year, month, day)
            except ValueError:
                continue
            value = value / 10.0
            records.append(
                {
                    "station_id": station_id,
                    "date": date.isoformat(),
                    "element": element,
                    "value": float(value),
                    "lat": station_meta["lat"],
                    "lon": station_meta["lon"],
                    "elevation": station_meta["elevation"],
                    "name": station_meta["name"],
                    "mflag": mflag.strip() or None,
                    "qflag": qflag.strip() or None,
                    "sflag": sflag.strip() or None,
                }
            )
    return records
