#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

from common import OUTPUT_ROOT, setup_logging


def fmt_ratio(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}x"


def fmt_float(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4f}"


def main() -> None:
    logger = setup_logging("report")
    metrics_path = OUTPUT_ROOT / "evaluation" / "metrics.json"
    if not metrics_path.exists():
        raise FileNotFoundError(f"Missing metrics: {metrics_path}")

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    latency = metrics.get("latency_seconds", {})
    notes = metrics.get("notes", {})
    storage_human = metrics.get("storage_human", {})
    compression = metrics.get("compression_ratio", {})
    dq = metrics.get("data_quality", {})

    report = [
        "# ABD Pipeline Report",
        "",
        "## Overview",
        "- Architecture: Medallion (Bronze → Silver → Gold) + DuckDB baseline.",
        "- Stack: Spark Standalone + MinIO (S3) + DuckDB.",
        "",
        "## Datasets",
        "- NASA GISTEMP v4 (monthly global temperature anomaly, CSV).",
        "- Global Carbon Budget 2024 (XLSX).",
        "- Copernicus ERA5 ASEAN subset (NetCDF).",
        "- NOAA GHCN-Daily + station metadata (TXT).",
        "",
        "## Pipeline Summary",
        "- Bronze: raw files uploaded to MinIO.",
        "- Silver: dataset-specific cleaning and normalization to Parquet.",
        "- Gold: aggregated temperature trend, carbon budget annual, and multisource join.",
        "- Baseline: DuckDB transforms directly from raw files.",
        "",
        "## Metrics",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Medallion query latency (s) | {fmt_float(latency.get('medallion_query'))} |",
        f"| Baseline query latency (s) | {fmt_float(latency.get('baseline_query'))} |",
        f"| Bronze size | {storage_human.get('bronze', 'n/a')} |",
        f"| Silver size | {storage_human.get('silver', 'n/a')} |",
        f"| Gold size | {storage_human.get('gold', 'n/a')} |",
        f"| Compression Bronze→Silver | {fmt_ratio(compression.get('bronze_to_silver'))} |",
        f"| Compression Silver→Gold | {fmt_ratio(compression.get('silver_to_gold'))} |",
        "",
        "## Notes",
        f"- {notes.get('latency', 'n/a')}",
        "",
        "## Data Quality Scores",
        "| Dataset | Raw | Silver |",
        "| --- | --- | --- |",
        f"| NASA | {fmt_float(dq.get('nasa', {}).get('raw_score'))} | {fmt_float(dq.get('nasa', {}).get('silver_score'))} |",
        f"| GCB | {fmt_float(dq.get('gcb', {}).get('raw_score'))} | {fmt_float(dq.get('gcb', {}).get('silver_score'))} |",
        f"| ERA5 | {fmt_float(dq.get('era5', {}).get('raw_score'))} | {fmt_float(dq.get('era5', {}).get('silver_score'))} |",
        f"| NOAA | {fmt_float(dq.get('noaa', {}).get('raw_score'))} | {fmt_float(dq.get('noaa', {}).get('silver_score'))} |",
        f"| Overall | {fmt_float(dq.get('overall', {}).get('raw_score'))} | {fmt_float(dq.get('overall', {}).get('silver_score'))} |",
        "",
        "## Analysis",
        "- Medallion typically improves query consistency by separating raw and curated layers.",
        "- Higher medallion latency than baseline is acceptable here because the design favors governance and reprocessability over raw point-query speed.",
        "- Use the metrics above to compare latency and storage trade-offs against the baseline.",
        "",
        "## Conclusions",
        "- Summarize which approach is preferable based on your metric outcomes.",
    ]

    out_path = OUTPUT_ROOT / "report.md"
    out_path.write_text("\n".join(report), encoding="utf-8")
    logger.info("Report written to %s", out_path)


if __name__ == "__main__":
    main()
