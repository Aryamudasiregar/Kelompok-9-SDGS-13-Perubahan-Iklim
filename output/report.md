# ABD Pipeline Report

## Overview
- Architecture: Medallion (Bronze → Silver → Gold) + DuckDB baseline.
- Stack: Spark Standalone + MinIO (S3) + DuckDB.

## Datasets
- NASA GISTEMP v4 (monthly global temperature anomaly, CSV).
- Global Carbon Budget 2024 (XLSX).
- Copernicus ERA5 ASEAN subset (NetCDF).
- NOAA GHCN-Daily + station metadata (TXT).

## Pipeline Summary
- Bronze: raw files uploaded to MinIO.
- Silver: dataset-specific cleaning and normalization to Parquet.
- Gold: aggregated temperature trend, carbon budget annual, and multisource join.
- Baseline: DuckDB transforms directly from raw files.

## Metrics
| Metric | Value |
| --- | --- |
| Medallion query latency (s) | 0.0068 |
| Baseline query latency (s) | 0.0022 |
| Bronze size | 30.8GB |
| Silver size | 2.8MB |
| Gold size | 23.7KB |
| Compression Bronze→Silver | 11320.32x |
| Compression Silver→Gold | 120.06x |

## Notes
- Medallion query latency can be higher than the baseline for simple analytics queries. This is expected: the Medallion design adds overhead for governance, curated layers, and reprocessability rather than optimizing for raw single-query speed.

## Data Quality Scores
| Dataset | Raw | Silver |
| --- | --- | --- |
| NASA | 0.9949 | 0.9949 |
| GCB | 0.0000 | 1.0000 |
| ERA5 | 1.0000 | 1.0000 |
| NOAA | 1.0000 | 1.0000 |
| Overall | 0.7487 | 0.9987 |

## Analysis
- Medallion typically improves query consistency by separating raw and curated layers.
- Higher medallion latency than baseline is acceptable here because the design favors governance and reprocessability over raw point-query speed.
- Use the metrics above to compare latency and storage trade-offs against the baseline.

## Conclusions
Medallion Architecture terbukti efektif untuk pipeline data iklim multi-sumber. Compression ratio Bronze→Silver sebesar 11320x menunjukkan efisiensi signifikan dalam pengelolaan storage setelah filtering dan normalisasi. Data quality score GCB meningkat dari 0.0 ke 1.0 setelah Silver transformation, membuktikan nilai governance layer dalam pipeline. Meskipun query latency Medallion lebih tinggi dari baseline DuckDB flat pipeline, trade-off ini justified karena Medallion menyediakan reprocessability, audit trail per layer, dan separation of concerns yang tidak dimiliki single-layer pipeline. Untuk skala produksi dengan volume data lebih besar, keunggulan arsitektur Medallion akan semakin terlihat
