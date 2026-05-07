"""PyFlink Table API 에 Iceberg (Lakekeeper REST) catalog 를 등록한다."""
from __future__ import annotations

from pyflink.table import TableEnvironment

from platform_common import get_settings


def register_iceberg_catalog(t_env: TableEnvironment, catalog_alias: str = "ice") -> str:
    """Lakekeeper REST → Iceberg catalog 등록. catalog alias 반환."""
    s = get_settings()
    rest_uri = f"{s.lakekeeper_url}/catalog"
    # Lakekeeper REST 는 warehouse 를 NAME (e.g. "seoul") 으로 lookup. S3 path 가 아님.
    # Lakekeeper 의 storage-profile (bucket / key-prefix / endpoint) 은 등록 시점 정의.
    warehouse = s.iceberg_catalog_name

    ddl = f"""
    CREATE CATALOG {catalog_alias} WITH (
      'type' = 'iceberg',
      'catalog-type' = 'rest',
      'uri' = '{rest_uri}',
      'warehouse' = '{warehouse}',
      'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
      's3.endpoint' = '{s.minio_endpoint}',
      's3.access-key-id' = '{s.minio_user}',
      's3.secret-access-key' = '{s.minio_password.get_secret_value()}',
      's3.path-style-access' = 'true',
      's3.region' = '{s.minio_region}',
      'header.X-Iceberg-Access-Delegation' = 'vended-credentials'
    )
    """
    # Flink 1.19 의 SQL parser 는 CREATE CATALOG IF NOT EXISTS 미지원
    # (FLINK-32777 가 1.20 부터 반영). list_catalogs 로 존재 체크 후 conditional 생성.
    if catalog_alias not in t_env.list_catalogs():
        t_env.execute_sql(ddl)
    t_env.execute_sql(f"USE CATALOG {catalog_alias}")
    # Flink SQL 의 fully-qualified name 은 catalog.database.table 3 부분.
    # Iceberg REST 의 nested namespace (seoul.bronze) 와 다름 — flat database 로 단순화.
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {catalog_alias}.bronze")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {catalog_alias}.silver")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {catalog_alias}.gold")
    return catalog_alias


def warehouse_namespace() -> str:
    """`{catalog}.{db}` 의 catalog 부분 반환 (Lakekeeper warehouse 이름)."""
    return get_settings().iceberg_catalog_name
