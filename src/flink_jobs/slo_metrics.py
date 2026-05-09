"""Day 4 Task 4.2 — 데이터 신선도 SLO 측정 (P95 < 7분).

서울 OpenAPI 응답 `tm` (= producer 가 attach 한 `api_response_ts`) 부터
Iceberg Gold 도달 (`gold_arrival_ts`, `silver_to_gold.py` 의
`CURRENT_TIMESTAMP`) 까지의 지연 분포를 계산. spec §6-2 가 단일 출처.

본 모듈은 두 영역으로 분리된다.

- 순수 함수: `compute_freshness_seconds`, `summarize`, `_percentile`.
  Iceberg / pyiceberg 의존이 없어서 단위 테스트가 빠르고 결정적.
- 실 데이터 fetch: `fetch_samples_from_gold`. pyiceberg + DuckDB 우회.
  plan(L2476~L2502) 원본 (DuckDB `iceberg_scan(s3://...)`) 이 두 가지로
  동작 안 함 — (1) Lakekeeper 가 vend 하는 UUID-prefix path 를 iceberg_scan
  이 resolve 못함 (Task 4.1 verification 에서 확인), (2) pyiceberg
  `t.scan().to_arrow()` 도 pyarrow 11 (PyFlink 1.20 transitive) 의
  `concat_tables(promote_options=...)` 미지원으로 fail. 회피 — pyiceberg
  `plan_files()` 로 실제 parquet path 받아 DuckDB `read_parquet
  (hive_partitioning=true)` 로 직접 read. 상세는 함수 docstring 참조.

Run:
  JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \\
    uv run --extra flink python -m flink_jobs.slo_metrics
"""
from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

#: SLO 임계값 (P95 < 7분, spec §6-2).
SLO_P95_SECONDS = 7 * 60


@dataclass
class FreshnessReport:
    """SLO 리포트 1회 측정 스냅샷."""

    count: int
    p50_seconds: int
    p95_seconds: int
    p99_seconds: int
    max_seconds: int
    slo_violated: bool


def compute_freshness_seconds(api_ts: datetime, gold_ts: datetime) -> int:
    """`gold_arrival_ts - api_response_ts` 를 초 단위 정수로 반환.

    음수 (clock skew / out-of-order) 는 0 으로 clamp.
    """
    delta = (gold_ts - api_ts).total_seconds()
    return max(0, int(delta))


def _percentile(sorted_samples: Sequence[int], p: float) -> int:
    """Linear interpolation percentile (rank = p * (n-1)).

    Empty → 0. 부동소수점 epsilon 안정화 위해 `int(round(...))`.
    `numpy.percentile(..., method='linear')` 와 동일 결과.
    """
    if not sorted_samples:
        return 0
    n = len(sorted_samples)
    rank = p * (n - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return int(sorted_samples[lo])
    frac = rank - lo
    val = sorted_samples[lo] + (sorted_samples[hi] - sorted_samples[lo]) * frac
    return int(round(val))


def summarize(samples: Sequence[int]) -> FreshnessReport:
    """Sample list → FreshnessReport. 빈 입력은 모두 0 / not violated."""
    if not samples:
        return FreshnessReport(
            count=0,
            p50_seconds=0,
            p95_seconds=0,
            p99_seconds=0,
            max_seconds=0,
            slo_violated=False,
        )
    s = sorted(samples)
    p50 = _percentile(s, 0.50)
    p95 = _percentile(s, 0.95)
    p99 = _percentile(s, 0.99)
    return FreshnessReport(
        count=len(s),
        p50_seconds=p50,
        p95_seconds=p95,
        p99_seconds=p99,
        max_seconds=int(s[-1]),
        slo_violated=p95 > SLO_P95_SECONDS,
    )


def fetch_samples_from_gold() -> list[int]:
    """`gold.fact_hotspot_congestion_5min` 최근 24시간 row 의 freshness sec 리스트.

    catalog 생성 / parquet path 조회 / DuckDB SECRET 설정의 3 단계는
    `flink_jobs.lib.duckdb_iceberg` 가 담당. 회피 배경 (DuckDB
    `iceberg_scan` 의 UUID-prefix path 미해결 + pyarrow 11
    `concat_tables(promote_options=...)` 미지원) 은 lib 모듈 docstring.
    """
    # Lazy import — duckdb / pyiceberg 는 dev/flink extra 에서만 보장.
    from contextlib import closing

    import duckdb

    from flink_jobs.lib.duckdb_iceberg import (
        build_catalog,
        configure_duckdb,
        table_paths,
    )

    catalog = build_catalog()
    file_paths = table_paths(catalog, "gold.fact_hotspot_congestion_5min")
    if not file_paths:
        return []

    cutoff = (datetime.now(UTC) - timedelta(hours=24)).replace(tzinfo=None)
    with closing(duckdb.connect()) as con:
        configure_duckdb(con)
        rows = con.execute(
            """
            SELECT date_diff('second', last_api_response_ts, gold_arrival_ts) AS sec
            FROM read_parquet(?, hive_partitioning = true)
            WHERE last_api_response_ts IS NOT NULL
              AND gold_arrival_ts IS NOT NULL
              AND gold_arrival_ts >= ?
            """,
            [file_paths, cutoff],
        ).fetchall()
    return [max(0, int(r[0])) for r in rows if r[0] is not None]


def main() -> None:
    samples = fetch_samples_from_gold()
    rep = summarize(samples)
    print("== Freshness SLO Report ==")
    print(f"count          : {rep.count}")
    print(f"p50 seconds    : {rep.p50_seconds}")
    print(f"p95 seconds    : {rep.p95_seconds}  (SLO threshold: {SLO_P95_SECONDS})")
    print(f"p99 seconds    : {rep.p99_seconds}")
    print(f"max seconds    : {rep.max_seconds}")
    print(f"SLO violated   : {rep.slo_violated}")


if __name__ == "__main__":
    main()
