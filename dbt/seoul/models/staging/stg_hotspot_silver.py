"""dbt python model — silver hotspot Iceberg → DuckDB staging.

plan Task 5.2 deviation B 적용 — DuckDB `iceberg_scan(plain path)` 이
Lakekeeper REST 의 UUID-prefix path 를 resolve 못하는 학습 (Day 4 archive
`2026-05-09-day-4-tasks-4_1-4_3.md`) 재현 확인 후 `flink_jobs.lib.duckdb_iceberg`
의 pyiceberg `plan_files()` + DuckDB `read_parquet()` 우회 패턴 활용.

핵심 deviation 3건 (Task 5.2 commit body 에 명문화):

1. .sql → .py (python model). lib 동적 lookup 사용.
2. view → table materialize. python model 자체 제약 + DuckDB in-memory
   환경에선 storage 차이 0 (매 dbt run 마다 새 process) 이라 비용 무시.
3. silver `score > 0` filter 를 staging 에서 mart 로 이동. singular
   test (Task 5.3) 가 staging ref 직접 사용해 score = 0 row 까지 검증
   가능하게 하기 위함.
"""
from __future__ import annotations


def model(dbt, session):
    dbt.config(materialized="table")

    from flink_jobs.lib.duckdb_iceberg import (
        build_catalog,
        configure_duckdb,
        table_paths,
    )

    catalog = build_catalog()
    file_paths = table_paths(catalog, "silver.hotspot_congestion")

    if not file_paths:
        return session.sql(
            """
            SELECT
                CAST(NULL AS VARCHAR) AS area_code,
                CAST(NULL AS VARCHAR) AS area_name,
                CAST(NULL AS VARCHAR) AS district,
                CAST(NULL AS VARCHAR) AS gu_code,
                CAST(NULL AS DOUBLE) AS latitude,
                CAST(NULL AS DOUBLE) AS longitude,
                CAST(NULL AS VARCHAR) AS congest_level,
                CAST(NULL AS INTEGER) AS congest_level_score,
                CAST(NULL AS INTEGER) AS population_min,
                CAST(NULL AS INTEGER) AS population_max,
                CAST(NULL AS VARCHAR) AS road_traffic_index,
                CAST(NULL AS DOUBLE) AS road_traffic_speed_kmh,
                CAST(NULL AS DOUBLE) AS temperature_c,
                CAST(NULL AS TIMESTAMP) AS api_response_ts,
                CAST(NULL AS TIMESTAMP) AS silver_arrival_ts
            WHERE 1 = 0
            """
        )

    configure_duckdb(session)

    return session.sql(
        f"""
        SELECT
            area_code,
            area_name,
            district,
            gu_code,
            latitude,
            longitude,
            congest_level,
            congest_level_score,
            population_min,
            population_max,
            road_traffic_index,
            road_traffic_speed_kmh,
            temperature_c,
            api_response_ts,
            silver_arrival_ts
        FROM read_parquet({file_paths!r}, hive_partitioning = true)
        """
    )
