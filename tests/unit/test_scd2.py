"""SCD Type 2 골격 변환 — Debezium envelope → SCD2 row pure function 검증.

Day 6 Task 6.3 의 TDD 단계 — 실패 → 구현 → 통과. Flink job 자체는 PyFlink
런타임 의존이라 단위 테스트 대상 아님. envelope 파싱 / row 변환만 본 모듈에서 보장.
"""
from datetime import UTC, datetime

from flink_jobs.lib.scd2 import (
    Scd2Row,
    parse_debezium_envelope,
    to_scd2_row,
)


def _envelope(op: str, after: dict | None, before: dict | None = None, ts_ms: int = 1714490000000):
    return {"op": op, "before": before, "after": after, "ts_ms": ts_ms}


def test_parse_debezium_envelope_create():
    after = {"place_id": 1, "biz_reg_no": "BR1", "name": "A", "category": "카페",
             "district": "강남구", "gu_code": "11680",
             "latitude": 37.5, "longitude": 127.0,
             "open_hour": 9, "close_hour": 22, "status": "active",
             "created_at": "2026-04-30T00:00:00Z", "updated_at": "2026-04-30T00:00:00Z"}
    rec = parse_debezium_envelope(_envelope("c", after))
    assert rec is not None
    assert rec.op == "c"
    assert rec.payload["place_id"] == 1


def test_parse_debezium_envelope_delete_uses_before():
    before = {"place_id": 2, "name": "X", "biz_reg_no": "BR2",
              "category": "음식점", "district": "마포구", "gu_code": "11440",
              "latitude": 37.5, "longitude": 126.9,
              "open_hour": 0, "close_hour": 24, "status": "active",
              "created_at": "2026-04-30T00:00:00Z", "updated_at": "2026-04-30T00:00:00Z"}
    rec = parse_debezium_envelope(_envelope("d", after=None, before=before))
    assert rec is not None
    assert rec.op == "d"
    assert rec.payload["place_id"] == 2


def test_parse_debezium_envelope_returns_none_when_no_payload():
    assert parse_debezium_envelope(_envelope("u", after=None, before=None)) is None


def test_to_scd2_row_create_marks_current():
    after = {"place_id": 1, "biz_reg_no": "BR1", "name": "A", "category": "카페",
             "district": "강남구", "gu_code": "11680",
             "latitude": 37.5, "longitude": 127.0,
             "open_hour": 9, "close_hour": 22, "status": "active",
             "created_at": "2026-04-30T00:00:00Z", "updated_at": "2026-04-30T00:00:00Z"}
    rec = parse_debezium_envelope(_envelope("c", after, ts_ms=1714490000000))
    row = to_scd2_row(rec)
    assert isinstance(row, Scd2Row)
    assert row.place_id == 1
    assert row.is_current is True
    # ts_ms=1714490000000 → 2024-04-30 15:13:20 UTC. naive UTC 로 변환.
    assert row.valid_from == datetime(2024, 4, 30, 15, 13, 20, tzinfo=UTC).replace(tzinfo=None)
    assert row.valid_to is None
    assert row.cdc_op == "c"


def test_to_scd2_row_delete_marks_not_current():
    before = {"place_id": 2, "biz_reg_no": "BR2", "name": "X", "category": "음식점",
              "district": "마포구", "gu_code": "11440",
              "latitude": 37.5, "longitude": 126.9,
              "open_hour": 0, "close_hour": 24, "status": "active",
              "created_at": "2026-04-30T00:00:00Z", "updated_at": "2026-04-30T00:00:00Z"}
    rec = parse_debezium_envelope(_envelope("d", after=None, before=before, ts_ms=1714490000000))
    row = to_scd2_row(rec)
    assert row.is_current is False
    assert row.cdc_op == "d"
