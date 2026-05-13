"""Day 10 PR α — Iceberg `gold.fact_hotspot_congestion_5min` schema migration.

silver→gold 의 Platform Latency SLO 측정을 위해 `last_silver_arrival_ts` 컬럼 추가.
spec §6-2 정정 (두 종 SLO 분리) 의 source of truth.

**Path B 결정 (작업 도중 root cause 발견)**: silver Iceberg catalog 에 `kafka_ts`
컬럼이 미존재 (bronze 의 Kafka metadata 가 bronze→silver INSERT 시 silver 의
`silver_arrival_ts = CURRENT_TIMESTAMP` 로 대체). 따라서 Platform Latency 정의를
`gold_arrival_ts - silver_arrival_ts` (= silver→gold 의 우리 통제 구간 lag) 로 수정.
bronze→silver lag 미포함 한계는 Phase 1B/2 의 silver schema 정정 시점에 `kafka_ts`
ADD COLUMN 으로 해결 (deferred-items-post-day10 memory).

본 스크립트는 3 case 모두 멱등 처리:
- (a) `last_silver_arrival_ts` 이미 있음 → no-op
- (b) `last_kafka_ts` 만 있음 (1차 migration 잔여) → DROP + ADD (type 도 정정,
      TimestamptzType → TimestampType, silver_arrival_ts 의 silver schema TIMESTAMP(3) 정합)
- (c) 둘 다 없음 → ADD COLUMN

Run:
    uv run --extra flink python scripts/migrate_gold_add_kafka_ts.py
"""
from __future__ import annotations

import logging
import sys

from pyiceberg.types import TimestampType

from flink_jobs.lib.duckdb_iceberg import build_catalog

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("migrate_gold_platform_latency")

TABLE_QUALIFIED = "gold.fact_hotspot_congestion_5min"
TARGET_COLUMN = "last_silver_arrival_ts"
LEGACY_COLUMN = "last_kafka_ts"  # Day 10 PR α 1차 시도 잔여 (silver kafka_ts 미존재 발견 전)
COLUMN_DOC = (
    "Silver Iceberg 적재 시각 (`CURRENT_TIMESTAMP AS silver_arrival_ts` in "
    "bronze→silver Flink job), MAX over 5min tumbling window. Platform Latency "
    "SLO = gold_arrival_ts - silver_arrival_ts (= silver→gold 의 우리 통제 구간)."
)


def main() -> int:
    catalog = build_catalog()
    table = catalog.load_table(TABLE_QUALIFIED)
    names = {field.name for field in table.schema().fields}

    if TARGET_COLUMN in names:
        log.info("already exists: %s.%s — no-op", TABLE_QUALIFIED, TARGET_COLUMN)
        return 0

    if LEGACY_COLUMN in names:
        # DROP + ADD (type 정정: TimestamptzType → TimestampType, silver schema 정합).
        # 분리된 2 commit 으로 안전성 ↑ (pyiceberg same-commit drop+add 의 type
        # validation 우회).
        with table.update_schema() as update:
            update.delete_column(LEGACY_COLUMN)
        table.refresh()
        with table.update_schema() as update:
            update.add_column(TARGET_COLUMN, TimestampType(), doc=COLUMN_DOC)
        log.info(
            "Replaced: %s (TimestamptzType) → %s (TimestampType) in %s",
            LEGACY_COLUMN,
            TARGET_COLUMN,
            TABLE_QUALIFIED,
        )
        return 0

    with table.update_schema() as update:
        update.add_column(TARGET_COLUMN, TimestampType(), doc=COLUMN_DOC)
    log.info("Schema migrated: %s added to %s", TARGET_COLUMN, TABLE_QUALIFIED)
    return 0


if __name__ == "__main__":
    sys.exit(main())
