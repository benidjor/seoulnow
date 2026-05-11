"""Debezium ``place.master.cdc.v1`` → Iceberg ``silver.dim_place`` (append SCD2 골격).

valid_to 닫기는 Day 9 Spark MERGE 가 담당. 본 job 은 streaming append-only.

Run:
  uv run --extra flink python -m flink_jobs.cdc_to_dim_place

선결 조건:
  - infra/flink/download_jars.sh 1회 실행 (Maven Central JAR 5종 다운로드)
  - /etc/hosts 에 ``127.0.0.1 minio`` 추가 — Lakekeeper REST 가 vending 하는
    ``http://minio:9000`` 엔드포인트를 host Iceberg client 가 resolve 가능해야 함
  - docker compose 5종 (kafka / kafka-connect / lakekeeper / minio / postgres) healthy
  - Day 6 Task 6.1~6.2 완료 — places 시드 5건 + Debezium connector RUNNING 상태

deviation A — Debezium envelope wrapping (2026-05-11 PR β):
  PR α 검증에서 실제 토픽 message 가 ``{schema, payload}`` wrapping 으로 발행됨
  (``VALUE_CONVERTER_SCHEMAS_ENABLE=false`` 효력 안 남). 따라서 source DDL 의
  root field 는 ``payload`` ROW 1개만이며, ``op``/``before``/``after``/``ts_ms`` 는
  ``payload`` 안에서 unwrap.
"""

from __future__ import annotations

import logging
import os

from pyflink.table import TableEnvironment

from flink_jobs.lib.env import build_streaming_env
from flink_jobs.lib.iceberg_sink import register_iceberg_catalog
from flink_jobs.lib.lifecycle import wait_for_shutdown

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def register_cdc_source(t_env: TableEnvironment) -> None:
    """Kafka source ``place_cdc_src`` 등록.

    Debezium envelope ``{schema, payload}`` wrapping 구조를 풀기 위해 root 에
    ``payload`` ROW 만 둔다. ``op`` / ``ts_ms`` / ``before`` / ``after`` 는 모두
    ``payload`` 안에서 unwrap.
    """
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    ddl = f"""
    CREATE TEMPORARY TABLE place_cdc_src (
      `payload` ROW<
        `op` STRING,
        ts_ms BIGINT,
        `before` ROW<
          place_id BIGINT, biz_reg_no STRING, name STRING, category STRING,
          district STRING, gu_code STRING,
          latitude DOUBLE, longitude DOUBLE,
          open_hour INT, close_hour INT, status STRING
        >,
        `after` ROW<
          place_id BIGINT, biz_reg_no STRING, name STRING, category STRING,
          district STRING, gu_code STRING,
          latitude DOUBLE, longitude DOUBLE,
          open_hour INT, close_hour INT, status STRING
        >
      >
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'place.master.cdc.v1',
      'properties.bootstrap.servers' = '{bootstrap}',
      'properties.group.id' = 'flink-cdc-dim-place',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json',
      'json.ignore-parse-errors' = 'true'
    )
    """
    t_env.execute_sql(ddl)


def create_dim_place_table(t_env: TableEnvironment) -> None:
    """Iceberg ``silver.dim_place`` 테이블 생성 (없으면).

    SCD2 골격 — 각 CDC 이벤트마다 한 행 append. ``valid_to`` 는 Day 9 Spark
    MERGE 가 채우고, ``is_current`` 는 streaming 시점에 ``op != 'd'`` 로만 표시
    (직전 행을 false 로 닫는 작업도 Day 9 Spark 책임).

    iceberg_sink.register_iceberg_catalog 가 ``ice.{bronze,silver,gold}`` flat
    database 로 register 하므로 4-part (``ice.<warehouse>.silver.x``) 금지 —
    bronze_to_silver / silver_to_gold 와 동일한 3-part identifier 사용.
    """
    t_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS ice.silver.dim_place (
          place_id BIGINT,
          biz_reg_no STRING,
          name STRING,
          category STRING,
          district STRING,
          gu_code STRING,
          latitude DOUBLE,
          longitude DOUBLE,
          open_hour INT,
          close_hour INT,
          status STRING,
          cdc_op STRING,
          valid_from TIMESTAMP(3),
          valid_to TIMESTAMP(3),
          is_current BOOLEAN
        ) PARTITIONED BY (district)
        WITH ('format-version' = '2')
        """
    )


def run() -> None:
    t_env = build_streaming_env()
    register_iceberg_catalog(t_env, catalog_alias="ice")

    register_cdc_source(t_env)
    create_dim_place_table(t_env)

    # deviation A — payload.op / payload.`before|after`.* 로 unwrap.
    # COALESCE 로 delete 시 before, 그 외 after 우선 채택 (parse_debezium_envelope
    # pure function 의 SQL 등가물).
    t_env.execute_sql(
        """
        INSERT INTO ice.silver.dim_place
        SELECT
          COALESCE(`payload`.`after`.place_id, `payload`.`before`.place_id)         AS place_id,
          COALESCE(`payload`.`after`.biz_reg_no, `payload`.`before`.biz_reg_no)     AS biz_reg_no,
          COALESCE(`payload`.`after`.name, `payload`.`before`.name)                 AS name,
          COALESCE(`payload`.`after`.category, `payload`.`before`.category)         AS category,
          COALESCE(`payload`.`after`.district, `payload`.`before`.district)         AS district,
          COALESCE(`payload`.`after`.gu_code, `payload`.`before`.gu_code)           AS gu_code,
          COALESCE(`payload`.`after`.latitude, `payload`.`before`.latitude)         AS latitude,
          COALESCE(`payload`.`after`.longitude, `payload`.`before`.longitude)       AS longitude,
          COALESCE(`payload`.`after`.open_hour, `payload`.`before`.open_hour)       AS open_hour,
          COALESCE(`payload`.`after`.close_hour, `payload`.`before`.close_hour)     AS close_hour,
          COALESCE(`payload`.`after`.status, `payload`.`before`.status)             AS status,
          `payload`.`op`                                                            AS cdc_op,
          TO_TIMESTAMP_LTZ(`payload`.ts_ms, 3)                                      AS valid_from,
          CAST(NULL AS TIMESTAMP(3))                                                AS valid_to,
          (`payload`.`op` <> 'd')                                                   AS is_current
        FROM place_cdc_src
        WHERE `payload`.`op` IN ('c','u','d','r')
        """
    )

    log.info("CDC streaming job submitted (place.master.cdc.v1 → silver.dim_place)")
    # PyFlink LocalEnvironment 는 detached mode 미지원 — main 종료 = background job 종료.
    wait_for_shutdown()


if __name__ == "__main__":
    run()
