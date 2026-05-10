"""Kafka(`seoul.hotspot.congestion.v1`) → Iceberg bronze → silver streaming job.

Run (smoke):
  JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \\
    uv run --extra flink python -m flink_jobs.bronze_to_silver

선결 조건:
  - infra/flink/download_jars.sh 1회 실행 (Maven Central 5 JAR 다운로드 + PyFlink lib 동기화)
  - /etc/hosts 에 `127.0.0.1 minio` 추가 — Lakekeeper REST 가 vending 하는
    `http://minio:9000` 엔드포인트를 호스트의 Iceberg client 가 resolve 가능해야 함
  - docker compose 4종 (kafka / lakekeeper / minio / postgres) healthy
  - JDK 17 (Eclipse Temurin) 설치 + JAVA_HOME 설정
"""
from __future__ import annotations

import logging
import os
import time

from pyflink.table import DataTypes, TableEnvironment
from pyflink.table.udf import udtf

from flink_jobs.lib.env import build_streaming_env
from flink_jobs.lib.iceberg_sink import register_iceberg_catalog
from flink_jobs.lib.transforms import enrich_hotspot_silver

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

SMOKE_RUN_SECONDS = int(os.environ.get("FLINK_SMOKE_RUN_SECONDS", "600"))


def register_kafka_source_hotspot(t_env: TableEnvironment) -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    ddl = f"""
    CREATE TEMPORARY TABLE hotspot_kafka_src (
      area_code STRING,
      area_name STRING,
      congest_level STRING,
      congest_message STRING,
      population_min INT,
      population_max INT,
      road_traffic_index STRING,
      road_traffic_speed_kmh DOUBLE,
      temperature_c DOUBLE,
      precipitation STRING,
      api_response_ts TIMESTAMP(3),
      kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'seoul.hotspot.congestion.v1',
      'properties.bootstrap.servers' = '{bootstrap}',
      'properties.group.id' = 'flink-bronze-hotspot',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json',
      'json.timestamp-format.standard' = 'ISO-8601',
      'json.ignore-parse-errors' = 'true'
    )
    """
    t_env.execute_sql(ddl)


def create_bronze_table(t_env: TableEnvironment) -> None:
    t_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS ice.bronze.hotspot_raw (
          area_code STRING,
          area_name STRING,
          congest_level STRING,
          congest_message STRING,
          population_min INT,
          population_max INT,
          road_traffic_index STRING,
          road_traffic_speed_kmh DOUBLE,
          temperature_c DOUBLE,
          precipitation STRING,
          api_response_ts TIMESTAMP(3),
          kafka_ts TIMESTAMP_LTZ(3),
          ingest_ts TIMESTAMP_LTZ(3)
        ) PARTITIONED BY (area_code)
        WITH ('format-version' = '2', 'write.upsert.enabled' = 'false')
        """
    )


def create_silver_table(t_env: TableEnvironment) -> None:
    t_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS ice.silver.hotspot_congestion (
          area_code STRING,
          area_name STRING,
          district STRING,
          gu_code STRING,
          latitude DOUBLE,
          longitude DOUBLE,
          congest_level STRING,
          congest_level_score INT,
          congest_message STRING,
          population_min INT,
          population_max INT,
          road_traffic_index STRING,
          road_traffic_speed_kmh DOUBLE,
          temperature_c DOUBLE,
          precipitation STRING,
          api_response_ts TIMESTAMP(3),
          silver_arrival_ts TIMESTAMP(3)
        ) PARTITIONED BY (district)
        WITH ('format-version' = '2')
        """
    )


@udtf(result_types=DataTypes.ROW(
    [
        DataTypes.FIELD("district", DataTypes.STRING()),
        DataTypes.FIELD("gu_code", DataTypes.STRING()),
        DataTypes.FIELD("latitude", DataTypes.DOUBLE()),
        DataTypes.FIELD("longitude", DataTypes.DOUBLE()),
        DataTypes.FIELD("congest_level_score", DataTypes.INT()),
    ]
))
def enrich_tf(area_code: str, congest_level: str):
    """Table function. region 매핑 성공 시 1 row, 미매핑 시 0 row (자동 drop)."""
    bronze = {"area_code": area_code, "congest_level": congest_level,
              "population_min": None, "population_max": None,
              "api_response_ts": None}
    silver = enrich_hotspot_silver(bronze)
    if silver is None:
        return  # 빈 yield → row 자체 drop
    yield (
        silver["district"],
        silver["gu_code"],
        silver["latitude"],
        silver["longitude"],
        silver["congest_level_score"],
    )


def run() -> None:
    t_env = build_streaming_env()
    register_iceberg_catalog(t_env, catalog_alias="ice")

    register_kafka_source_hotspot(t_env)
    create_bronze_table(t_env)
    create_silver_table(t_env)
    t_env.create_temporary_function("enrich_hotspot", enrich_tf)

    # StatementSet 으로 두 INSERT 를 atomically submit.
    # 별도 execute_sql 2회 호출 시 첫 INSERT 가 background 로 가기 전에 두 번째 시도가
    # 충돌 가능. StatementSet 은 두 job graph 를 한 번에 검증 후 동시 submit.
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(
        """
        INSERT INTO ice.bronze.hotspot_raw
        SELECT
          area_code, area_name, congest_level, congest_message,
          population_min, population_max,
          road_traffic_index, road_traffic_speed_kmh,
          temperature_c, precipitation,
          api_response_ts, kafka_ts,
          CURRENT_TIMESTAMP AS ingest_ts
        FROM hotspot_kafka_src
        """
    )
    stmt_set.add_insert_sql(
        """
        INSERT INTO ice.silver.hotspot_congestion
        SELECT
          b.area_code,
          b.area_name,
          e.district,
          e.gu_code,
          e.latitude,
          e.longitude,
          b.congest_level,
          e.congest_level_score,
          b.congest_message,
          b.population_min,
          b.population_max,
          b.road_traffic_index,
          b.road_traffic_speed_kmh,
          b.temperature_c,
          b.precipitation,
          b.api_response_ts,
          CURRENT_TIMESTAMP AS silver_arrival_ts
        FROM ice.bronze.hotspot_raw /*+ OPTIONS('streaming'='true', 'monitor-interval'='30s') */ b
        CROSS JOIN LATERAL TABLE(enrich_hotspot(b.area_code, b.congest_level))
          AS e(district, gu_code, latitude, longitude, congest_level_score)
        """
    )
    stmt_set.execute()
    log.info("Bronze + Silver streaming jobs submitted (StatementSet)")
    # PyFlink LocalEnvironment 는 detached mode 미지원 — main 종료 = background job 종료.
    # smoke 검증 + checkpoint commit (30초 간격) 을 위해 main 에서 명시적 대기.
    # SIGTERM 받으면 즉시 종료. 운영 시점에는 별도 deploy mode (per-job cluster) 로 변경 검토.
    log.info("Streaming 가동 중. SIGTERM 대기 (최대 %ds).", SMOKE_RUN_SECONDS)
    time.sleep(SMOKE_RUN_SECONDS)
    log.info("Smoke run timeout, exiting.")


if __name__ == "__main__":
    run()
