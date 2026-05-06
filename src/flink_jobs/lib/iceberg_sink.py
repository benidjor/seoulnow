"""PyFlink Table API 에 Iceberg (Lakekeeper REST) catalog 를 등록한다."""
from __future__ import annotations

from pyflink.table import TableEnvironment

from platform_common import get_settings


def register_iceberg_catalog(t_env: TableEnvironment, catalog_alias: str = "ice") -> str:
    """Lakekeeper REST → Iceberg catalog 등록. catalog alias 반환."""
    s = get_settings()
    rest_uri = f"{s.lakekeeper_url}/catalog"
    warehouse = f"s3://{s.iceberg_warehouse_bucket}/warehouse"

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
      's3.region' = '{s.minio_region}'
    )
    """
    t_env.execute_sql(ddl)
    t_env.execute_sql(f"USE CATALOG {catalog_alias}")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {s.iceberg_catalog_name}.bronze")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {s.iceberg_catalog_name}.silver")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {s.iceberg_catalog_name}.gold")
    return catalog_alias


def warehouse_namespace() -> str:
    """`{catalog}.{db}` 의 catalog 부분 반환 (Lakekeeper warehouse 이름)."""
    return get_settings().iceberg_catalog_name
