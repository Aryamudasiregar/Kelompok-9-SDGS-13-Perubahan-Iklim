#!/usr/bin/env python3
from __future__ import annotations

import json
import tarfile
import time
from pathlib import Path

import duckdb
import pandas as pd

from common import (
    BRONZE_BUCKET,
    DATA_ROOT,
    GOLD_BUCKET,
    NOAA_ELEMENTS,
    NASA_MONTHS,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    OUTPUT_ROOT,
    SILVER_BUCKET,
    extract_zip,
    load_gcb_raw_df,
    load_nasa_raw_df,
    load_noaa_bounds,
    load_noaa_inventory,
    load_noaa_stations,
    parse_noaa_dly_lines,
    s3_client,
    setup_logging,
    summarize_era5_nc,
)

ERA5_ZIP = DATA_ROOT / "copernicus" / "era5_asean_2023_2024.zip"
ERA5_DIR = DATA_ROOT / "copernicus" / "era5_asean_2023_2024"
NOAA_TAR = DATA_ROOT / "noaa" / "ghcnd_all.tar.gz"
NOAA_STATIONS = DATA_ROOT / "noaa" / "ghcnd-stations.txt"
NOAA_INVENTORY = DATA_ROOT / "noaa" / "ghcnd-inventory.txt"


def bucket_size_bytes(s3, bucket: str) -> int:
    total = 0
    token = None
    while True:
        params = {"Bucket": bucket}
        if token:
            params["ContinuationToken"] = token
        resp = s3.list_objects_v2(**params)
        for item in resp.get("Contents", []):
            total += int(item["Size"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return total


def human_size(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024:
            return f"{num:.1f}{unit}"
        num /= 1024
    return f"{num:.1f}PB"


def calc_nasa_quality(raw_df: pd.DataFrame, silver_df: pd.DataFrame) -> dict:
    raw_vals = raw_df[NASA_MONTHS]
    raw_total = raw_vals.size
    raw_non_null = raw_vals.notna().sum().sum()
    raw_completeness = raw_non_null / raw_total if raw_total else 0.0
    raw_valid = raw_vals.apply(pd.to_numeric, errors="coerce")
    raw_valid_ratio = ((raw_valid >= -5) & (raw_valid <= 5)).sum().sum() / raw_total if raw_total else 0.0

    silver_total = len(silver_df)
    silver_non_null = silver_df["anomaly"].notna().sum()
    silver_completeness = silver_non_null / silver_total if silver_total else 0.0
    silver_valid_ratio = (
        ((silver_df["anomaly"] >= -5) & (silver_df["anomaly"] <= 5)).sum() / silver_total
        if silver_total
        else 0.0
    )

    return {
        "raw": {"completeness": raw_completeness, "validity": raw_valid_ratio},
        "silver": {"completeness": silver_completeness, "validity": silver_valid_ratio},
    }


def calc_gcb_quality(raw_df: pd.DataFrame, silver_df: pd.DataFrame) -> dict:
    raw_numeric = raw_df.select_dtypes(include=["number"])
    raw_total = raw_numeric.size
    raw_non_null = raw_numeric.notna().sum().sum()
    raw_completeness = raw_non_null / raw_total if raw_total else 0.0

    silver_total = len(silver_df)
    silver_non_null = silver_df["value"].notna().sum()
    silver_completeness = silver_non_null / silver_total if silver_total else 0.0

    return {
        "raw": {"completeness": raw_completeness, "validity": raw_completeness},
        "silver": {"completeness": silver_completeness, "validity": silver_completeness},
    }


def build_era5_df() -> pd.DataFrame:
    nc_files: list[Path] = []
    if ERA5_DIR.exists():
        nc_files = list(ERA5_DIR.rglob("*.nc"))
    if not nc_files:
        if not ERA5_ZIP.exists():
            raise FileNotFoundError("ERA5 zip not found.")
        nc_files = extract_zip(ERA5_ZIP, OUTPUT_ROOT / "tmp" / "evaluation" / "era5")
    records: list[dict] = []
    for path in nc_files:
        records.extend(summarize_era5_nc(path))
    return pd.DataFrame(records)


def load_noaa_daily() -> pd.DataFrame:
    lat_min, lat_max, lon_min, lon_max, max_stations = load_noaa_bounds()
    stations = load_noaa_stations(NOAA_STATIONS, lat_min, lat_max, lon_min, lon_max)
    eligible = load_noaa_inventory(NOAA_INVENTORY, NOAA_ELEMENTS)
    stations = [s for s in stations if s["station_id"] in eligible][:max_stations]
    station_map = {s["station_id"]: s for s in stations}

    if not station_map:
        raise ValueError("No stations matched the bounding box filter.")
    if not NOAA_TAR.exists():
        raise FileNotFoundError(f"NOAA archive not found: {NOAA_TAR}")

    records: list[dict] = []
    with tarfile.open(NOAA_TAR, "r:gz") as tar:
        members = {Path(m.name).name: m for m in tar.getmembers() if m.name.endswith(".dly")}
        for station_id, meta in station_map.items():
            member = members.get(f"{station_id}.dly")
            if not member:
                continue
            fileobj = tar.extractfile(member)
            if not fileobj:
                continue
            lines = fileobj.read().decode("utf-8").splitlines()
            records.extend(parse_noaa_dly_lines(lines, meta, NOAA_ELEMENTS))

    return pd.DataFrame(records)


def calc_era5_quality(raw_df: pd.DataFrame, silver_df: pd.DataFrame) -> dict:
    raw_total = len(raw_df)
    raw_values = pd.to_numeric(raw_df["value"], errors="coerce")
    raw_times = pd.to_datetime(raw_df["time"], errors="coerce")
    raw_valid = (raw_values.notna() & raw_times.notna() & raw_df["variable"].notna()).sum()
    raw_completeness = raw_values.notna().sum() / raw_total if raw_total else 0.0
    raw_validity = raw_valid / raw_total if raw_total else 0.0

    silver_total = len(silver_df)
    silver_values = pd.to_numeric(silver_df["value"], errors="coerce")
    silver_times = pd.to_datetime(silver_df["time"], errors="coerce")
    silver_valid = (silver_values.notna() & silver_times.notna() & silver_df["variable"].notna()).sum()
    silver_completeness = silver_values.notna().sum() / silver_total if silver_total else 0.0
    silver_validity = silver_valid / silver_total if silver_total else 0.0

    return {
        "raw": {"completeness": raw_completeness, "validity": raw_validity},
        "silver": {"completeness": silver_completeness, "validity": silver_validity},
    }


def calc_noaa_quality(raw_df: pd.DataFrame, silver_df: pd.DataFrame) -> dict:
    raw_total = len(raw_df)
    raw_values = pd.to_numeric(raw_df["value"], errors="coerce")
    raw_dates = pd.to_datetime(raw_df["date"], errors="coerce")
    raw_valid = (raw_values.notna() & raw_dates.notna() & raw_df["element"].isin(NOAA_ELEMENTS)).sum()
    raw_completeness = raw_values.notna().sum() / raw_total if raw_total else 0.0
    raw_validity = raw_valid / raw_total if raw_total else 0.0

    silver_total = len(silver_df)
    silver_values = pd.to_numeric(silver_df["value"], errors="coerce")
    silver_dates = pd.to_datetime(silver_df["date"], errors="coerce")
    silver_valid = (silver_values.notna() & silver_dates.notna() & silver_df["element"].isin(NOAA_ELEMENTS)).sum()
    silver_completeness = silver_values.notna().sum() / silver_total if silver_total else 0.0
    silver_validity = silver_valid / silver_total if silver_total else 0.0

    return {
        "raw": {"completeness": raw_completeness, "validity": raw_validity},
        "silver": {"completeness": silver_completeness, "validity": silver_validity},
    }


def main() -> None:
    logger = setup_logging("evaluate")
    out_dir = OUTPUT_ROOT / "evaluation"
    out_dir.mkdir(parents=True, exist_ok=True)

    s3 = s3_client()
    bronze_size = bucket_size_bytes(s3, BRONZE_BUCKET)
    silver_size = bucket_size_bytes(s3, SILVER_BUCKET)
    gold_size = bucket_size_bytes(s3, GOLD_BUCKET)

    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT.replace('http://', '').replace('https://', '')}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")

    gold_temp = "s3://gold/climate/temp_trend/*.parquet"
    baseline_temp = OUTPUT_ROOT / "baseline" / "temp_trend.parquet"

    gold_query = f"""
        SELECT date, avg(value) AS avg_value
        FROM parquet_scan('{gold_temp}')
        WHERE source = 'NASA_GISTEMP'
        GROUP BY date
        ORDER BY date
    """
    baseline_query = f"""
        SELECT date, avg(value) AS avg_value
        FROM parquet_scan('{baseline_temp.as_posix()}')
        WHERE source = 'NASA_GISTEMP'
        GROUP BY date
        ORDER BY date
    """

    start = time.perf_counter()
    con.execute(gold_query).fetchall()
    gold_time = time.perf_counter() - start

    start = time.perf_counter()
    con.execute(baseline_query).fetchall()
    baseline_time = time.perf_counter() - start

    explain_output = con.execute("EXPLAIN ANALYZE " + gold_query).fetchall()
    explain_text = "\n".join(row[0] for row in explain_output)
    (out_dir / "explain_analyze_gold.txt").write_text(explain_text, encoding="utf-8")

    raw_nasa = load_nasa_raw_df(DATA_ROOT / "nasa" / "GLB.Ts+dSST.csv")
    raw_gcb = load_gcb_raw_df(DATA_ROOT / "gcb" / "Global_Carbon_Budget_2024_v1.0-3.xlsx")
    raw_era5 = build_era5_df()
    raw_noaa = load_noaa_daily()

    silver_nasa_local = OUTPUT_ROOT / "silver" / "nasa_gistemp"
    silver_gcb_local = OUTPUT_ROOT / "silver" / "gcb"
    silver_era5_local = OUTPUT_ROOT / "silver" / "era5"
    silver_noaa_local = OUTPUT_ROOT / "silver" / "noaa_daily"
    if silver_nasa_local.exists():
        silver_nasa = con.execute(f"SELECT * FROM parquet_scan('{silver_nasa_local.as_posix()}')").df()
    else:
        silver_nasa = con.execute("SELECT * FROM parquet_scan('s3://silver/nasa/gistemp/*.parquet')").df()

    if silver_gcb_local.exists():
        silver_gcb = con.execute(f"SELECT * FROM parquet_scan('{silver_gcb_local.as_posix()}')").df()
    else:
        silver_gcb = con.execute("SELECT * FROM parquet_scan('s3://silver/gcb/global_carbon_budget/*.parquet')").df()

    if silver_era5_local.exists():
        silver_era5 = con.execute(f"SELECT * FROM parquet_scan('{silver_era5_local.as_posix()}')").df()
    else:
        silver_era5 = con.execute("SELECT * FROM parquet_scan('s3://silver/copernicus/era5_asean/*.parquet')").df()

    if silver_noaa_local.exists():
        silver_noaa = con.execute(f"SELECT * FROM parquet_scan('{silver_noaa_local.as_posix()}')").df()
    else:
        silver_noaa = con.execute("SELECT * FROM parquet_scan('s3://silver/noaa/ghcnd_daily/*.parquet')").df()

    nasa_quality = calc_nasa_quality(raw_nasa, silver_nasa)
    gcb_quality = calc_gcb_quality(raw_gcb, silver_gcb)
    era5_quality = calc_era5_quality(raw_era5, silver_era5)
    noaa_quality = calc_noaa_quality(raw_noaa, silver_noaa)

    def score(q: dict) -> float:
        return 0.5 * q["completeness"] + 0.5 * q["validity"]

    dq_summary = {
        "nasa": {"raw_score": score(nasa_quality["raw"]), "silver_score": score(nasa_quality["silver"])},
        "gcb": {"raw_score": score(gcb_quality["raw"]), "silver_score": score(gcb_quality["silver"])},
        "era5": {"raw_score": score(era5_quality["raw"]), "silver_score": score(era5_quality["silver"])},
        "noaa": {"raw_score": score(noaa_quality["raw"]), "silver_score": score(noaa_quality["silver"])},
    }
    dq_summary["overall"] = {
        "raw_score": (
            dq_summary["nasa"]["raw_score"]
            + dq_summary["gcb"]["raw_score"]
            + dq_summary["era5"]["raw_score"]
            + dq_summary["noaa"]["raw_score"]
        )
        / 4,
        "silver_score": (
            dq_summary["nasa"]["silver_score"]
            + dq_summary["gcb"]["silver_score"]
            + dq_summary["era5"]["silver_score"]
            + dq_summary["noaa"]["silver_score"]
        )
        / 4,
    }

    metrics = {
        "latency_seconds": {"medallion_query": gold_time, "baseline_query": baseline_time},
        "notes": {
            "latency": (
                "Medallion query latency can be higher than the baseline for simple analytics queries. "
                "This is expected: the Medallion design adds overhead for governance, curated layers, "
                "and reprocessability rather than optimizing for raw single-query speed."
            )
        },
        "storage_bytes": {"bronze": bronze_size, "silver": silver_size, "gold": gold_size},
        "storage_human": {
            "bronze": human_size(bronze_size),
            "silver": human_size(silver_size),
            "gold": human_size(gold_size),
        },
        "compression_ratio": {
            "bronze_to_silver": bronze_size / silver_size if silver_size else None,
            "silver_to_gold": silver_size / gold_size if gold_size else None,
        },
        "data_quality": dq_summary,
    }

    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    logger.info("Evaluation metrics written to %s", out_dir / "metrics.json")


if __name__ == "__main__":
    main()
