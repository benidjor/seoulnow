"""SCD Type 2 골격 변환 — Debezium envelope → Iceberg `silver.dim_place` row.

본 모듈은 PyFlink 의존이 전혀 없는 pure function 모음. PyFlink job
(`flink_jobs.cdc_to_dim_place`) 에서 직접 호출하기보다 Flink SQL INSERT 가
Debezium envelope 의 `before` / `after` ROW 를 풀어 적재하는 reference
정의 역할을 겸한다 (= row 변환 의도가 단위 테스트로 고정).

valid_to 닫기 / SCD2 폐쇄는 Day 9 Spark MERGE 가 담당. 본 모듈은 streaming
append-only 변환만 다룬다.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class CdcRecord:
    """Debezium envelope 1개를 정규화한 형태.

    `op`:
      - ``c`` : create (INSERT)
      - ``u`` : update
      - ``d`` : delete (payload 는 before 에서 복사)
      - ``r`` : snapshot read (Debezium 초기 스냅샷)
    """

    op: str
    payload: dict[str, Any]
    ts_ms: int


@dataclass(frozen=True)
class Scd2Row:
    """`silver.dim_place` 의 한 행 — SCD2 골격 (append-only)."""

    place_id: int
    biz_reg_no: str
    name: str
    category: str
    district: str
    gu_code: str
    latitude: float | None
    longitude: float | None
    open_hour: int | None
    close_hour: int | None
    status: str
    cdc_op: str
    valid_from: datetime
    valid_to: datetime | None
    is_current: bool


def parse_debezium_envelope(env: dict[str, Any]) -> CdcRecord | None:
    """Debezium JSON envelope → ``CdcRecord``.

    유효하지 않은 op 또는 payload 누락 시 None 반환 — Flink SQL 의
    ``json.ignore-parse-errors=true`` 와 정렬되는 정책. 단위 테스트에서 envelope
    structure 변경 회귀를 잡는다.
    """
    op = env.get("op")
    if op not in ("c", "u", "d", "r"):
        return None
    payload = env.get("after") if op != "d" else env.get("before")
    if not payload:
        return None
    return CdcRecord(op=op, payload=payload, ts_ms=int(env.get("ts_ms", 0)))


def to_scd2_row(rec: CdcRecord) -> Scd2Row:
    """``CdcRecord`` → ``Scd2Row``.

    - ``valid_from`` = Debezium ts_ms 를 naive UTC datetime 으로 변환. dataclass
      비교 안정성 (tz-aware 와 naive 는 비교 불가) + Flink ``TIMESTAMP(3)`` 컬럼
      과 정렬.
    - ``valid_to`` = None — Day 9 Spark MERGE 가 직전 행을 닫을 때 채움.
    - ``is_current`` = ``op != 'd'`` — delete 면 false, 그 외 true.
    """
    p = rec.payload
    valid_from = datetime.fromtimestamp(rec.ts_ms / 1000, tz=UTC).replace(tzinfo=None)
    return Scd2Row(
        place_id=int(p["place_id"]),
        biz_reg_no=str(p.get("biz_reg_no") or ""),
        name=str(p.get("name") or ""),
        category=str(p.get("category") or ""),
        district=str(p.get("district") or ""),
        gu_code=str(p.get("gu_code") or ""),
        latitude=p.get("latitude"),
        longitude=p.get("longitude"),
        open_hour=p.get("open_hour"),
        close_hour=p.get("close_hour"),
        status=str(p.get("status") or "active"),
        cdc_op=rec.op,
        valid_from=valid_from,
        valid_to=None,
        is_current=(rec.op != "d"),
    )
