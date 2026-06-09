# Pipeline Data Iklim (Medallion Architecture)

## Gambaran Umum

Pipeline Big Data berbasis Medallion Architecture untuk analisis data iklim multi-sumber menggunakan Apache Spark, MinIO, dan DuckDB.

```
Bronze (raw) → Silver (cleaned Parquet) → Gold (aggregated Parquet)
                                         ↕
                              Baseline: DuckDB flat pipeline
```

## Stack

| Komponen                  | Fungsi                                      |
| ------------------------- | ------------------------------------------- |
| Apache Spark (local mode) | ETL & transformasi                          |
| MinIO                     | Data lakehouse storage (Bronze/Silver/Gold) |
| DuckDB                    | Query, validasi, evaluasi                   |
| Docker Compose            | Orkestrasi MinIO                            |
| Python 3.12 + uv          | Scripting & manajemen dependensi            |

## Dataset

| Dataset                      | Format       | Ukuran  |
| ---------------------------- | ------------ | ------- |
| NASA GISTEMP v4              | CSV          | 12.6 KB |
| Global Carbon Budget 2024    | XLSX         | 919 KB  |
| Copernicus ERA5 ASEAN Subset | NetCDF       | 3 MB    |
| NOAA GHCN-Daily              | TXT + TAR.GZ | ~4 GB   |

## Hasil Pipeline

| Layer  | Ukuran  |
| ------ | ------- |
| Bronze | 30.8 GB |
| Silver | 2.8 MB  |
| Gold   | 23.7 KB |

- Compression Bronze→Silver: **11320x**
- Data quality GCB: **0.0 → 1.0** setelah Silver transformation
- Overall data quality: **0.75 → 0.999**

Lihat detail di [`output/evaluation/metrics.json`](output/evaluation/metrics.json) dan [`output/report.md`](output/report.md).

## Setup

### Prasyarat

- Docker + Docker Compose
- Python 3.12 + [uv](https://github.com/astral-sh/uv)
- Java 17 (direkomendasikan untuk kompatibilitas PySpark)

### Menjalankan Pipeline

```bash
# 1. Clone repo
git clone https://github.com/Aryamuda/Climate-Data-Pipeline-Medallion-Architecture-.git
cd Climate-Data-Pipeline-Medallion-Architecture

# 2. Start MinIO
docker compose up -d

# 3. Install dependensi
uv sync

# 4. Jalankan pipeline
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

MinIO console tersedia di `http://localhost:9001`  
Login: `minioadmin` / `minioadmin`

## Struktur Repo

```
scripts/          — pipeline scripts 
logs/             — execution logs
output/
  report.md       — laporan pipeline & metrik
  evaluation/
    metrics.json  — raw evaluation metrics
  silver/         — cleaned Parquet per dataset
  gold/           — aggregated Parquet
docker-compose.yml
pyproject.toml
```
