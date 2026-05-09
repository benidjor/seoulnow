"""Day 4 Task 4.3 — DuckDB + pyiceberg 로 Bronze/Silver/Gold 3-layer 검증.

catalog 생성 / parquet path 조회 / DuckDB SECRET 설정 3 helper 는
`flink_jobs.lib.duckdb_iceberg` 가 담당 — `slo_metrics.fetch_samples_from_gold`
도 같은 lib 호출. 회피 배경 (DuckDB `iceberg_scan` UUID-prefix path
미해결 + pyarrow 11 `concat_tables(promote_options=...)` 미지원) 은 lib
모듈 docstring.

Run:
  JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \\
    uv run --extra flink python scripts/duckdb_check.py
"""
from __future__ import annotations

from contextlib import closing

import duckdb

from flink_jobs.lib.duckdb_iceberg import build_catalog, configure_duckdb, table_paths


def main() -> None:
    catalog = build_catalog()

    with closing(duckdb.connect()) as con:
        configure_duckdb(con)

        # 1) Bronze count
        bronze_paths = table_paths(catalog, "bronze.hotspot_raw")
        bronze_count = 0
        if bronze_paths:
            row = con.execute(
                "SELECT COUNT(*) FROM read_parquet(?, hive_partitioning = true)",
                [bronze_paths],
            ).fetchone()
            if row is not None:
                bronze_count = row[0]
        print("== bronze.hotspot_raw count ==")
        print((bronze_count,))

        # 2) Silver 최근 5행
        silver_paths = table_paths(catalog, "silver.hotspot_congestion")
        silver_rows: list[tuple] = []
        if silver_paths:
            silver_rows = con.execute(
                """
                SELECT area_code, area_name, district, congest_level,
                       congest_level_score, population_min, population_max,
                       api_response_ts, silver_arrival_ts
                FROM read_parquet(?, hive_partitioning = true)
                ORDER BY silver_arrival_ts DESC
                LIMIT 5
                """,
                [silver_paths],
            ).fetchall()
        print()
        print("== silver.hotspot_congestion sample ==")
        for row in silver_rows:
            print(row)

        # 3) Gold 최근 5행
        gold_paths = table_paths(catalog, "gold.fact_hotspot_congestion_5min")
        gold_rows: list[tuple] = []
        if gold_paths:
            gold_rows = con.execute(
                """
                SELECT window_start, window_end, district,
                       area_count, avg_congest_score
                FROM read_parquet(?, hive_partitioning = true)
                ORDER BY window_start DESC
                LIMIT 5
                """,
                [gold_paths],
            ).fetchall()
        print()
        print("== gold.fact_hotspot_congestion_5min sample ==")
        for row in gold_rows:
            print(row)

        # 4) district 별 latest avg_congest_score (DuckDB QUALIFY 사용).
        district_rows: list[tuple] = []
        if gold_paths:
            district_rows = con.execute(
                """
                SELECT district, avg_congest_score
                FROM read_parquet(?, hive_partitioning = true)
                QUALIFY row_number() OVER (
                    PARTITION BY district ORDER BY window_start DESC
                ) = 1
                ORDER BY avg_congest_score DESC
                """,
                [gold_paths],
            ).fetchall()
        print()
        print("== district 별 latest avg_congest_score ==")
        for row in district_rows:
            print(row)


if __name__ == "__main__":
    main()
