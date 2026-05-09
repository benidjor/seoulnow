"""Lakekeeper REST + DuckDB 우회 패턴 helper.

`slo_metrics.fetch_samples_from_gold` / `scripts/duckdb_check`
(+ Day 10 `slo_daily_report` DAG 예정) 의 공통 3 단계.

회피 배경 (Day 4 Task 4.1 verification):

- DuckDB `iceberg_scan(s3://...)` — Lakekeeper REST 가 vend 하는
  UUID-prefix path 를 resolve 못함.
- `pyiceberg.scan().to_arrow()` — pyarrow 11 (PyFlink 1.20 transitive)
  에서 `concat_tables(promote_options=...)` TypeError. promote_options
  는 pyarrow 14+ 추가.

회피 — pyiceberg `plan_files()` 로 실제 parquet path 받아서 DuckDB
`read_parquet(?, hive_partitioning=true)` 로 직접 read. pyiceberg 는
catalog lookup, DuckDB 는 parquet decode 로 책임 분리.
"""
from __future__ import annotations

import duckdb
from pyiceberg.catalog import Catalog, load_catalog

from platform_common import get_settings


def build_catalog() -> Catalog:
    """Lakekeeper REST catalog 1개 생성. 호출자가 여러 table 재사용 권장."""
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


def configure_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    """DuckDB 연결에 httpfs + S3 SECRET 1회 설정. 후속 read_parquet 들이 공유.

    DuckDB SECRET DDL 은 prepared parameter binding 미지원 (DDL 문법
    제약) — string interpolation 만 가능. credentials 는 dev settings
    에서만 주입되지만, single quote 가 포함된 case 회피 위해
    `_quote_literal` 로 escape — SQL injection 표면 0.
    """
    s = get_settings()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint_host = s.minio_endpoint.replace("http://", "").replace("https://", "")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET (
            TYPE S3,
            KEY_ID '{_quote_literal(s.minio_user)}',
            SECRET '{_quote_literal(s.minio_password.get_secret_value())}',
            ENDPOINT '{_quote_literal(endpoint_host)}',
            URL_STYLE 'path',
            USE_SSL false,
            REGION '{_quote_literal(s.minio_region)}'
        )
        """
    )


def table_paths(catalog: Catalog, qualified: str) -> list[str]:
    """`namespace.table` qualified name → parquet file path list.

    빈 table (= snapshot 0) 은 빈 list. 호출자가 falsy check 후
    DuckDB read 자체를 skip.
    """
    table = catalog.load_table(qualified)
    return [f.file.file_path for f in table.scan().plan_files()]


def _quote_literal(value: str) -> str:
    """DuckDB SQL string literal escape. single quote → 두 번 (`''`)."""
    return value.replace("'", "''")
