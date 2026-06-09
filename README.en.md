# Climate Data Pipeline (Medallion Architecture)

## Overview

A Big Data pipeline based on Medallion Architecture for multi-source climate data analysis using Apache Spark, MinIO, and DuckDB.

```
Bronze (raw) → Silver (cleaned Parquet) → Gold (aggregated Parquet)
                                         ↕
                              Baseline: DuckDB flat pipeline
```

## Stack

| Component                 | Role                                        |
| ------------------------- | ------------------------------------------- |
| Apache Spark (local mode) | ETL & transformation                        |
| MinIO                     | Data lakehouse storage (Bronze/Silver/Gold) |
| DuckDB                    | Query, validation, evaluation               |
| Docker Compose            | MinIO orchestration                         |
| Python 3.12 + uv          | Scripting & dependency management           |

## Datasets

| Dataset                      | Format       | Size    |
| ---------------------------- | ------------ | ------- |
| NASA GISTEMP v4              | CSV          | 12.6 KB |
| Global Carbon Budget 2024    | XLSX         | 919 KB  |
| Copernicus ERA5 ASEAN Subset | NetCDF       | 3 MB    |
| NOAA GHCN-Daily              | TXT + TAR.GZ | ~4 GB   |

## Pipeline Results

| Layer  | Size    |
| ------ | ------- |
| Bronze | 30.8 GB |
| Silver | 2.8 MB  |
| Gold   | 23.7 KB |

- Bronze→Silver compression: **11320x**
- GCB data quality: **0.0 → 1.0** after Silver transformation
- Overall data quality: **0.75 → 0.999**

See [`output/evaluation/metrics.json`](output/evaluation/metrics.json) and [`output/report.md`](output/report.md) for full details.

## Setup

### Prerequisites

- Docker + Docker Compose
- Python 3.12 + [uv](https://github.com/astral-sh/uv)
- Java 17 (recommended for PySpark compatibility)

### Running the Pipeline

```bash
# 1. Clone repo
git clone https://github.com/Aryamuda/Climate-Data-Pipeline-Medallion-Architecture-.git
cd Climate-Data-Pipeline-Medallion-Architecture

# 2. Start MinIO
docker compose up -d

# 3. Install dependencies
uv sync

# 4. Run pipeline
uv run scripts/bronze.py
uv run scripts/silver_nasa.py
uv run scripts/silver_gcb.py
uv run scripts/silver_era5.py
uv run scripts/silver_noaa.py
uv run scripts/gold_temp.py
uv run scripts/gold_carbon.py
uv run scripts/gold_join.py
uv run scripts/evaluate.py
uv run scripts/report.py
```

MinIO console available at `http://localhost:9001`  
Login: `minioadmin` / `minioadmin`

## Repository Structure

```
scripts/          — pipeline scripts
logs/             — execution logs
output/
  report.md       — pipeline report & metrics
  evaluation/
    metrics.json  — raw evaluation metrics
  silver/         — cleaned Parquet per dataset
  gold/           — aggregated Parquet
docker-compose.yml
pyproject.toml
```
