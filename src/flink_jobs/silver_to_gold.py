"""Iceberg silver(`hotspot_congestion`) → gold(`fact_hotspot_congestion_5min`).

5분 텀블링 윈도우. 자치구(district) 단위 평균/최대 혼잡도, 평균 인구.

Run (smoke):
  JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \\
    uv run --extra flink python -m flink_jobs.silver_to_gold

선결 조건:
  - bronze_to_silver streaming job 가동 중 (silver 적재 source 로 사용)
  - infra/flink/download_jars.sh 1회 실행 + /etc/hosts minio 매핑
  - docker compose 4종 (kafka / lakekeeper / minio / postgres) healthy
  - JDK 17 + JAVA_HOME

bronze_to_silver.py 와 동일하게 적용된 보강:
  - classloader.parent-first-patterns.additional 로 codahale/dropwizard
    metrics LinkageError 회피. 미적용 시 IcebergStreamWriter
    prepareSnapshotPreBarrier 단계에서 silent commit fail.
  - table.dynamic-table-options.enabled = true. silver streaming read
    의 SQL hint OPTIONS('streaming'='true', 'monitor-interval'='30s')
    적용을 위해 필요.
  - Iceberg silver source 위에 WATERMARK 부여. CREATE TEMPORARY VIEW 는
    WATERMARK 절을 못 갖기 때문에, 같은 Lakekeeper REST + S3FileIO 경로를
    direct connector 옵션으로 다시 박은 임시 source-table 에서 정의.
    미적용 시 TUMBLE 윈도우가 close 되지 않아 gold row 0 건.
  - Flink fully-qualified name 은 catalog.database.table 3 부분.
    iceberg_sink.register_iceberg_catalog 가 ice.{bronze,silver,gold}
    flat database 로 register 하므로 4-part (ice.seoul.silver.x) 금지.
"""
from __future__ import annotations

import logging
import os
import time

from pyflink.table import EnvironmentSettings, TableEnvironment

# TODO Day 5 (3번째 streaming job 진입) 전 `flink_jobs/lib/classpath.py` 로 이동.
# 현재 sibling module 의 `_` private 식별자를 import 하는 형태라 contract 가 모호.
from flink_jobs.bronze_to_silver import _classpath
from flink_jobs.lib.iceberg_sink import register_iceberg_catalog
from platform_common import get_settings

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

SMOKE_RUN_SECONDS = int(os.environ.get("FLINK_SMOKE_RUN_SECONDS", "600"))


def build_env() -> TableEnvironment:
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("pipeline.jars", _classpath())
    t_env.get_config().set("parallelism.default", "1")
    # bronze_to_silver 와 동일한 30초. silver INSERT commit 주기와 정렬.
    t_env.get_config().set("execution.checkpointing.interval", "30 s")
    # /*+ OPTIONS(...) */ SQL hint 적용을 위해 명시적 enable.
    t_env.get_config().set("table.dynamic-table-options.enabled", "true")
    # codahale/dropwizard metrics LinkageError 회피 (Day 4 Task 1 fix 동일).
    t_env.get_config().set(
        "classloader.parent-first-patterns.additional",
        "com.codahale.metrics.;io.dropwizard.metrics.",
    )
    return t_env


def create_gold_table(t_env: TableEnvironment) -> None:
    t_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS ice.gold.fact_hotspot_congestion_5min (
          window_start TIMESTAMP(3),
          window_end   TIMESTAMP(3),
          district STRING,
          gu_code STRING,
          area_count BIGINT,
          avg_congest_score DOUBLE,
          max_congest_score INT,
          avg_population_min DOUBLE,
          avg_population_max DOUBLE,
          last_api_response_ts TIMESTAMP(3),
          gold_arrival_ts TIMESTAMP(3)
        ) PARTITIONED BY (district)
        WITH ('format-version' = '2')
        """
    )


def create_silver_source_with_watermark(t_env: TableEnvironment) -> None:
    """silver `hotspot_congestion` 을 streaming + watermark 부여 source 로 등록.

    Iceberg silver catalog table 에는 watermark 가 없고, Flink SQL 의
    CREATE TEMPORARY VIEW 도 WATERMARK 절을 받지 못한다. 따라서 같은
    Lakekeeper REST + S3FileIO 옵션을 direct connector 옵션으로 다시 박은
    임시 source-table 을 만들어 watermark 를 부여한다. 물리 데이터는
    catalog table `ice.silver.hotspot_congestion` 과 동일.

    drift 주의: silver schema (`bronze_to_silver.py:create_silver_table` 17
    컬럼) 가 변경되면 본 함수의 7 컬럼 (area_code / district / gu_code /
    congest_level_score / population_min / population_max / api_response_ts)
    도 동기 수정 필요.
    """
    s = get_settings()
    t_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE silver_stream_wm (
          area_code STRING,
          district STRING,
          gu_code STRING,
          congest_level_score INT,
          population_min INT,
          population_max INT,
          api_response_ts TIMESTAMP(3),
          event_time AS api_response_ts,
          WATERMARK FOR event_time AS event_time - INTERVAL '1' MINUTE
        ) WITH (
          'connector' = 'iceberg',
          'catalog-name' = 'silver_src_wm',
          'catalog-type' = 'rest',
          'uri' = '{s.lakekeeper_url}/catalog',
          'warehouse' = '{s.iceberg_catalog_name}',
          'catalog-database' = 'silver',
          'catalog-table' = 'hotspot_congestion',
          'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
          's3.endpoint' = '{s.minio_endpoint}',
          's3.access-key-id' = '{s.minio_user}',
          's3.secret-access-key' = '{s.minio_password.get_secret_value()}',
          's3.path-style-access' = 'true',
          's3.region' = '{s.minio_region}',
          'streaming' = 'true',
          'monitor-interval' = '30s'
        )
        """
    )


def run() -> None:
    t_env = build_env()
    register_iceberg_catalog(t_env, catalog_alias="ice")
    create_gold_table(t_env)
    create_silver_source_with_watermark(t_env)

    insert_sql = """
        INSERT INTO ice.gold.fact_hotspot_congestion_5min
        SELECT
          window_start,
          window_end,
          district,
          MAX(gu_code) AS gu_code,
          COUNT(DISTINCT area_code) AS area_count,
          AVG(CAST(congest_level_score AS DOUBLE)) AS avg_congest_score,
          MAX(congest_level_score) AS max_congest_score,
          AVG(CAST(population_min AS DOUBLE)) AS avg_population_min,
          AVG(CAST(population_max AS DOUBLE)) AS avg_population_max,
          MAX(api_response_ts) AS last_api_response_ts,
          CURRENT_TIMESTAMP AS gold_arrival_ts
        FROM TABLE(
          TUMBLE(TABLE silver_stream_wm, DESCRIPTOR(event_time), INTERVAL '5' MINUTES)
        )
        GROUP BY window_start, window_end, district
    """
    t_env.execute_sql(insert_sql)
    log.info("Silver→Gold streaming job submitted (5min tumbling, district)")
    log.info("Streaming 가동 중. SIGTERM 대기 (최대 %ds).", SMOKE_RUN_SECONDS)
    time.sleep(SMOKE_RUN_SECONDS)
    log.info("Smoke run timeout, exiting.")


if __name__ == "__main__":
    run()
