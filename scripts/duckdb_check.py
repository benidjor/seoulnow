"""Day 4 Task 4.3 — DuckDB + pyiceberg 로 Bronze/Silver/Gold 3-layer 검증.

plan(L2541~L2625) 원본은 DuckDB `iceberg_scan('s3://seoul-warehouse/...')`
직접 호출이었으나 두 가지로 막힘 — (1) Lakekeeper REST 가 vend 하는
UUID-prefix path 를 iceberg_scan 이 resolve 못함 (Task 4.1 verification 기록),
(2) `pyiceberg.scan().to_arrow()` 도 pyarrow 11 (PyFlink 1.20 transitive)
조합에서 `concat_tables(promote_options=...)` 미지원 TypeError.

회피 — `slo_metrics.fetch_samples_from_gold` 와 동일 패턴.
pyiceberg `plan_files()` 로 실제 parquet path 를 받아서
DuckDB `read_parquet(hive_partitioning=true)` 로 직접 read.

Run:
  JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \\
    uv run --extra flink python scripts/duckdb_check.py
"""
from __future__ import annotations

from contextlib import closing

import duckdb
from pyiceberg.catalog import Catalog, load_catalog

from platform_common import get_settings


def _build_catalog() -> Catalog:
    """Lakekeeper REST catalog 1회 생성. 4 query 가 동일 catalog 재사용."""
    s = get_settings()
    return load_catalog(
        "rest",
        **{
            "uri": f"{s.lakekeeper_url}/catalog",
            "warehouse": s.iceberg_catalog_name,
            "s3.endpoint": s.minio_endpoint,
            "s3.access-key-id": s.minio_user,
            "s3.secret-access-key": s.minio_password.get_secret_value(),
            "s3.path-style-access": "true",
            "s3.region": s.minio_region,
        },
    )


def _configure_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    """httpfs + S3 SECRET 1회 설정. 후속 read_parquet 들이 공유."""
    s = get_settings()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint_host = s.minio_endpoint.replace("http://", "").replace("https://", "")
    # f-string 인라인 — credentials 가 항상 로컬 dev secret. SQL injection 표면 없음.
    con.execute(
        f"""
        CREATE OR REPLACE SECRET (
            TYPE S3,
            KEY_ID '{s.minio_user}',
            SECRET '{s.minio_password.get_secret_value()}',
            ENDPOINT '{endpoint_host}',
            URL_STYLE 'path',
            USE_SSL false,
            REGION '{s.minio_region}'
        )
        """
    )


def _table_paths(catalog: Catalog, qualified: str) -> list[str]:
    """`namespace.table` → parquet file path list. 빈 table 은 빈 리스트."""
    table = catalog.load_table(qualified)
    return [f.file.file_path for f in table.scan().plan_files()]


def main() -> None:
    catalog = _build_catalog()

    with closing(duckdb.connect()) as con:
        _configure_duckdb(con)

        # 1) Bronze count
        bronze_paths = _table_paths(catalog, "bronze.hotspot_raw")
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
        silver_paths = _table_paths(catalog, "silver.hotspot_congestion")
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
        gold_paths = _table_paths(catalog, "gold.fact_hotspot_congestion_5min")
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
