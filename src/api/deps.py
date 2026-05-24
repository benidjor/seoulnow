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

thread-safety (Task 11.1-A, 정찰 Issue 3): `@lru_cache` 싱글톤(DuckDB 연결 +
pyiceberg Catalog)을 FastAPI 동기 엔드포인트가 anyio threadpool 다중 스레드에서
공유 → 병렬 요청 502. DuckDB 는 요청별 `duck_cursor()` (싱글톤 DB 의 독립 cursor),
catalog 는 `threading.local()` 스레드 로컬 인스턴스로 분리.
"""

from __future__ import annotations

import threading
from functools import lru_cache

import duckdb
from pyiceberg.catalog import Catalog

from flink_jobs.lib.duckdb_iceberg import build_catalog, configure_duckdb, table_paths

_tls = threading.local()


@lru_cache
def duck_connection() -> duckdb.DuckDBPyConnection:
    """Process-singleton DuckDB DB. lib 가 SECRET DDL + httpfs LOAD 처리.

    요청별 실행은 duck_cursor() 의 .cursor() 로 분리 — DuckDBPyConnection 자체는
    thread-unsafe 이나 cursor 는 같은 DB 인스턴스 안에서 독립 실행 컨텍스트.
    """
    con = duckdb.connect()
    configure_duckdb(con)
    return con


def duck_cursor() -> duckdb.DuckDBPyConnection:
    """요청 스코프 cursor. threadpool 동시 요청이 같은 연결 객체를 공유하지 않게 함."""
    return duck_connection().cursor()


def catalog() -> Catalog:
    """Thread-local Lakekeeper REST catalog (pyiceberg Catalog 동시 접근 회피).

    build_catalog 0.09s — 스레드당 1회 생성. lru_cache 싱글톤이 threadpool 에서
    공유되며 502 를 유발한 Issue 3 의 해결.
    """
    cat = getattr(_tls, "catalog", None)
    if cat is None:
        cat = build_catalog()
        _tls.catalog = cat
    return cat


def silver_table_paths() -> list[str]:
    """`silver.hotspot_congestion` 의 parquet path list. 빈 list 면 snapshot 0."""
    return table_paths(catalog(), "silver.hotspot_congestion")


def gold_table_paths() -> list[str]:
    """`gold.fact_hotspot_congestion_5min` 의 parquet path list."""
    return table_paths(catalog(), "gold.fact_hotspot_congestion_5min")
