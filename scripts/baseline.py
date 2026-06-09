#!/usr/bin/env python3
from __future__ import annotations

import os
import tarfile
import time
from pathlib import Path

import duckdb
import pandas as pd

from common import (
    DATA_ROOT,
    NOAA_ELEMENTS,
    OUTPUT_ROOT,
    choose_match,
    extract_zip,
    load_gcb_long_df,
    load_nasa_long_df,
    load_noaa_bounds,
    load_noaa_inventory,
    load_noaa_stations,
    parse_noaa_dly_lines,
    setup_logging,
    summarize_era5_nc,
)

ERA5_ZIP = DATA_ROOT / "copernicus" / "era5_asean_2023_2024.zip"
ERA5_DIR = DATA_ROOT / "copernicus" / "era5_asean_2023_2024"

NOAA_TAR = DATA_ROOT / "noaa" / "ghcnd_all.tar.gz"
NOAA_STATIONS = DATA_ROOT / "noaa" / "ghcnd-stations.txt"
NOAA_INVENTORY = DATA_ROOT / "noaa" / "ghcnd-inventory.txt"

INCLUDE_NOAA = os.getenv("BASELINE_INCLUDE_NOAA", "1") == "1"


def build_era5_df() -> pd.DataFrame:
    nc_files: list[Path] = []
    if ERA5_DIR.exists():
        nc_files = list(ERA5_DIR.rglob("*.nc"))
    if not nc_files:
        if not ERA5_ZIP.exists():
            raise FileNotFoundError("ERA5 zip not found.")
        nc_files = extract_zip(ERA5_ZIP, OUTPUT_ROOT / "tmp" / "baseline" / "era5")
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


def build_baseline_tables(con, nasa_df: pd.DataFrame, gcb_df: pd.DataFrame, era5_df: pd.DataFrame) -> None:
    con.register("nasa_monthly", nasa_df)
    con.register("gcb_long", gcb_df)
    con.register("era5", era5_df)

    con.execute(
        """
        CREATE OR REPLACE TABLE temp_trend AS
        SELECT date, 'NASA_GISTEMP' AS source, 'temp_anomaly' AS variable, 'C' AS unit, anomaly AS value
        FROM nasa_monthly
        WHERE anomaly IS NOT NULL
        UNION ALL
        SELECT date_trunc('month', CAST(time AS TIMESTAMP)) AS date, 'ERA5' AS source, variable, units AS unit, avg(value) AS value
        FROM era5
        GROUP BY 1,2,3,4
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TABLE gcb_annual AS
        SELECT year, metric, avg(value) AS value
        FROM gcb_long
        GROUP BY year, metric
        """
    )


def build_join_table(con, era5_df: pd.DataFrame, gcb_df: pd.DataFrame) -> tuple[str, str]:
    era5_target = choose_match(era5_df["variable"].dropna().unique().tolist(), ["t2m", "temp", "temperature", "tas"])
    gcb_target = choose_match(gcb_df["metric"].dropna().unique().tolist(), ["fossil", "co2", "emission", "total"])
    if not era5_target or not gcb_target:
        raise ValueError("Unable to choose ERA5 variable or GCB metric for baseline join.")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE multi_source_join AS
        SELECT n.year,
               n.nasa_anomaly,
               e.era5_value,
               g.gcb_value,
               '{era5_target}' AS era5_variable,
               '{gcb_target}' AS gcb_metric
        FROM (
            SELECT year, avg(anomaly) AS nasa_anomaly
            FROM nasa_monthly
            GROUP BY year
        ) n
        LEFT JOIN (
            SELECT EXTRACT(YEAR FROM CAST(time AS TIMESTAMP)) AS year, avg(value) AS era5_value
            FROM era5
            WHERE variable = '{era5_target}'
            GROUP BY 1
        ) e
        ON n.year = e.year
        LEFT JOIN (
            SELECT year, avg(value) AS gcb_value
            FROM gcb_long
            WHERE metric = '{gcb_target}'
            GROUP BY year
        ) g
        ON n.year = g.year
        ORDER BY n.year
        """
    )
    return era5_target, gcb_target


def write_outputs(con, out_dir: Path) -> None:
    con.execute(f"COPY temp_trend TO '{out_dir / 'temp_trend.parquet'}' (FORMAT 'parquet')")
    con.execute(f"COPY gcb_annual TO '{out_dir / 'gcb_annual.parquet'}' (FORMAT 'parquet')")
    con.execute(f"COPY multi_source_join TO '{out_dir / 'multi_source_join.parquet'}' (FORMAT 'parquet')")


def main() -> None:
    logger = setup_logging("baseline")
    start = time.perf_counter()
    out_dir = OUTPUT_ROOT / "baseline"
    out_dir.mkdir(parents=True, exist_ok=True)

    nasa_df = load_nasa_long_df(DATA_ROOT / "nasa" / "GLB.Ts+dSST.csv")
    gcb_df = load_gcb_long_df(DATA_ROOT / "gcb" / "Global_Carbon_Budget_2024_v1.0-3.xlsx")
    era5_df = build_era5_df()

    con = duckdb.connect()
    build_baseline_tables(con, nasa_df, gcb_df, era5_df)
    build_join_table(con, era5_df, gcb_df)
    write_outputs(con, out_dir)

    if INCLUDE_NOAA:
        noaa_df = load_noaa_daily()
        con.register("noaa_daily", noaa_df)
        con.execute(
            """
            CREATE OR REPLACE TABLE noaa_daily_clean AS
            SELECT station_id, date::DATE AS date, element, value, lat, lon, elevation, name
            FROM noaa_daily
            """
        )
        con.execute(f"COPY noaa_daily_clean TO '{out_dir / 'noaa_daily.parquet'}' (FORMAT 'parquet')")

    elapsed = time.perf_counter() - start
    logger.info("Baseline outputs written to %s in %.1fs.", out_dir, elapsed)


if __name__ == "__main__":
    main()
