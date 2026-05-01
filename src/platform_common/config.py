"""환경 변수 → 강타입 Settings (pydantic-settings)."""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    seoul_openapi_key: str = Field(default="", alias="SEOUL_OPENAPI_KEY")
    seoul_subway_api_key: str = Field(default="", alias="SEOUL_SUBWAY_API_KEY")

    kafka_bootstrap_servers: str = Field(
        default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS"
    )

    hotspot_poll_interval_sec: int = Field(default=300, alias="HOTSPOT_POLL_INTERVAL_SEC")
    subway_poll_interval_sec: int = Field(default=60, alias="SUBWAY_POLL_INTERVAL_SEC")

    minio_endpoint: str = Field(default="http://localhost:9000", alias="MINIO_ENDPOINT")
    minio_region: str = Field(default="us-east-1", alias="MINIO_REGION")
    minio_user: str = Field(default="minioadmin", alias="MINIO_ROOT_USER")
    minio_password: SecretStr = Field(default=SecretStr("minioadmin"), alias="MINIO_ROOT_PASSWORD")
    iceberg_warehouse_bucket: str = Field(default="seoul-warehouse", alias="ICEBERG_WAREHOUSE_BUCKET")

    lakekeeper_url: str = Field(default="http://localhost:8181", alias="LAKEKEEPER_URL")
    iceberg_catalog_name: str = Field(default="seoul", alias="ICEBERG_CATALOG_NAME")


@lru_cache
def get_settings() -> Settings:
    return Settings()
