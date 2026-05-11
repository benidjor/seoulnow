"""공공 인허가 일반음식점/휴게음식점 1회 적재 → s3 bronze parquet.

Run:
    uv run python scripts/load_static_places.py [--csv path/to.csv]

deviation 8.1-C (사전 채택) — plan 본문 line 1626~1633 의 inline
`CREATE OR REPLACE SECRET ...` DDL 을 `flink_jobs.lib.duckdb_iceberg.configure_duckdb`
위임으로 변경. PR #28 lib 추출 + Day 7 archive §10-4 의 lib reuse 패턴
정착 (6번째 consumer = 본 스크립트). SQL injection 표면 lib 한 곳에서 처리.

Iceberg 정식 등록은 Day 9 Spark 일시 기동 시 `MIGRATE` 또는 `CREATE TABLE LIKE`
로 처리한다. 본 스크립트는 단순 parquet write 만 책임 — Task 8.2 의 mart 가
DuckDB `read_parquet()` 로 직접 읽도록 설계 (plan line 1654).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from flink_jobs.lib.duckdb_iceberg import configure_duckdb
from platform_common import get_settings

DEFAULT_CSV = Path(__file__).resolve().parents[1] / "data" / "reference" / "places_seed_sample.csv"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=str(DEFAULT_CSV))
    args = parser.parse_args()

    s = get_settings()
    con = duckdb.connect()
    # lib 위임 — httpfs LOAD + SECRET DDL 한 곳에서 처리 (deviation 8.1-C).
    configure_duckdb(con)

    con.execute("CREATE SCHEMA IF NOT EXISTS scratch")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE scratch.places_static AS
        SELECT * FROM read_csv_auto('{args.csv}', header=true)
        """
    )
    row = con.execute("SELECT count(*) FROM scratch.places_static").fetchone()
    rows = row[0] if row is not None else 0
    print(f"loaded {rows} rows from {args.csv}")

    # DuckDB 0.x 의 iceberg 쓰기는 미성숙 → parquet 로 직접 적재 후 Iceberg 메타데이터는
    # Day 9 Spark 일시 기동 시 갱신. Task 8.2 mart 는 본 parquet 를 직접 read_parquet 한다.
    out = f"s3://{s.iceberg_warehouse_bucket}/warehouse/bronze/places_static_v1/data.parquet"
    con.execute(f"COPY (SELECT * FROM scratch.places_static) TO '{out}' (FORMAT PARQUET)")
    print(f"wrote parquet: {out}")
    print("note: register iceberg metadata via spark or pyflink in next session.")


if __name__ == "__main__":
    main()
