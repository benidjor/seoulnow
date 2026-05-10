"""dbt python model — gold.dim_place (silver SCD2 의 current snapshot view).

Day 6 Task 6.4 deviation D — plan 의 SQL view (source('silver', 'dim_place') 참조)
는 dbt-duckdb adapter 가 Lakekeeper Iceberg source 의 external_location 을 자동
read 못함. Day 5 stg_hotspot_silver.py 와 동일한 우회 패턴 — pyiceberg `plan_files()`
로 actual S3 path 받아 DuckDB `read_parquet(?, hive_partitioning=true)` 로 직접 read.

핵심 결정:
- materialized="table" (python model 자체 제약 + DuckDB in-memory 환경에선 storage 차이 0)
- "현재 활성 가게" = row_number=1 over (partition by place_id order by valid_from desc)
  + cdc_op <> 'd' + status='active'
- empty source (snapshot 없음) → schema-only empty result (`WHERE 1=0`).
"""
from __future__ import annotations


def model(dbt, session):
    dbt.config(materialized="table", schema="gold")

    from flink_jobs.lib.duckdb_iceberg import (
        build_catalog,
        configure_duckdb,
        table_paths,
    )

    catalog = build_catalog()
    file_paths = table_paths(catalog, "silver.dim_place")

    if not file_paths:
        return session.sql(
            """
            SELECT
                CAST(NULL AS BIGINT)    AS place_id,
                CAST(NULL AS VARCHAR)   AS biz_reg_no,
                CAST(NULL AS VARCHAR)   AS name,
                CAST(NULL AS VARCHAR)   AS category,
                CAST(NULL AS VARCHAR)   AS district,
                CAST(NULL AS VARCHAR)   AS gu_code,
                CAST(NULL AS DOUBLE)    AS latitude,
                CAST(NULL AS DOUBLE)    AS longitude,
                CAST(NULL AS INTEGER)   AS open_hour,
                CAST(NULL AS INTEGER)   AS close_hour,
                CAST(NULL AS VARCHAR)   AS status,
                CAST(NULL AS TIMESTAMP) AS valid_from
            WHERE 1 = 0
            """
        )

    configure_duckdb(session)

    return session.sql(
        f"""
        WITH ranked AS (
            SELECT
                *,
                row_number() OVER (PARTITION BY place_id ORDER BY valid_from DESC) AS rn
            FROM read_parquet({file_paths!r}, hive_partitioning = true)
        )
        SELECT
            place_id,
            biz_reg_no,
            name,
            category,
            district,
            gu_code,
            latitude,
            longitude,
            open_hour,
            close_hour,
            status,
            valid_from
        FROM ranked
        WHERE rn = 1
          AND cdc_op <> 'd'
          AND status = 'active'
        """
    )
