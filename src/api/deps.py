"""DuckDB + Iceberg 접근 helper. lib/duckdb_iceberg 우회 패턴 위임.

deviation 7.2-A — plan 본문의 inline `CREATE OR REPLACE SECRET` DDL +
`warehouse_base()` 폐기. Day 4 archive (`2026-05-09-day-4-tasks-4_1-4_3.md`
plan deviation 4) 의 정착 패턴 (`flink_jobs.lib.duckdb_iceberg`) 위임.

회피 배경:
- DuckDB `iceberg_scan(s3://...)` 가 Lakekeeper REST 의 UUID-prefix
  warehouse path 를 resolve 못함 (Day 4 학습).
- pyiceberg `plan_files()` 로 실제 parquet path 를 받아서 DuckDB
  `read_parquet(?, hive_partitioning=true)` 로 직접 read.
- SECRET DDL 의 single-quote escape 도 lib `_quote_literal` 한 곳에서
  처리 (PR #28 학습).

`scripts/duckdb_check.py` / `slo_metrics.fetch_samples_from_gold` 와
같은 lib helper 를 공유 — 신규 SECRET DDL drift 위험 0.
"""

from __future__ import annotations

from functools import lru_cache

import duckdb
from pyiceberg.catalog import Catalog

from flink_jobs.lib.duckdb_iceberg import build_catalog, configure_duckdb, table_paths


@lru_cache
def duck_connection() -> duckdb.DuckDBPyConnection:
    """Process-singleton DuckDB connection. lib 가 SECRET DDL + httpfs LOAD 처리."""
    con = duckdb.connect()
    configure_duckdb(con)
    return con


@lru_cache
def _catalog() -> Catalog:
    """Process-singleton Lakekeeper REST catalog."""
    return build_catalog()


def silver_table_paths() -> list[str]:
    """`silver.hotspot_congestion` 의 parquet path list. 빈 list 면 snapshot 0."""
    return table_paths(_catalog(), "silver.hotspot_congestion")


def gold_table_paths() -> list[str]:
    """`gold.fact_hotspot_congestion_5min` 의 parquet path list."""
    return table_paths(_catalog(), "gold.fact_hotspot_congestion_5min")
