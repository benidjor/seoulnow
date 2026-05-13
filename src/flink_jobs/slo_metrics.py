"""Day 4 Task 4.2 + Day 10 PR α — 두 가지 SLO 측정.

Day 10 PR α 정정 (spec §6-2 SoT):

- **(α) Data Freshness SLO** = `gold_arrival_ts - api_response_ts(tm)` P95 < 45m
  (서울 OpenAPI source lag 포함, 사용자 관점 데이터 나이)
- **(β) Platform Latency SLO** = `gold_arrival_ts - silver_arrival_ts` P95 < 7m
  (silver→gold 우리 통제 구간, 1번 micro-batch 15분 대비 50%+ 개선). Path B 결정 —
  silver Iceberg catalog 의 `kafka_ts` 부재로 bronze→silver lag 미포함 (Phase 1B/2
  의 silver schema 정정 시점에 full coverage 가능).

본 모듈은 두 영역으로 분리된다.

- 순수 함수: `compute_freshness_seconds` / `compute_platform_latency_seconds` /
  `summarize_one` / `summarize_dual` / `_percentile`. pyiceberg / DuckDB 의존이
  없어서 단위 테스트가 빠르고 결정적.
- 실 데이터 fetch: `fetch_dual_samples_from_gold`. pyiceberg + DuckDB 우회
  (`lib.duckdb_iceberg` 위임). 회피 배경 (DuckDB iceberg_scan UUID prefix +
  pyarrow 11 concat_tables) 은 해당 lib docstring.

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

#: (α) Data Freshness SLO 임계값 — API tm → Gold P95 < 45분 (spec §6-2).
SLO_DATA_FRESHNESS_SECONDS = 45 * 60

#: (β) Platform Latency SLO 임계값 — silver_arrival_ts → Gold P95 < 7분 (spec §6-2, Path B).
SLO_PLATFORM_LATENCY_SECONDS = 7 * 60


@dataclass
class MetricSummary:
    """단일 SLO 의 분포 + 위반 여부."""

    name: str
    threshold_seconds: int
    count: int
    p50_seconds: int
    p95_seconds: int
    p99_seconds: int
    max_seconds: int
    slo_violated: bool


@dataclass
class SLOReport:
    """두 가지 SLO 의 합 (data freshness + platform latency)."""

    data_freshness: MetricSummary
    platform_latency: MetricSummary

    @property
    def any_violated(self) -> bool:
        """둘 중 하나라도 위반이면 True. branch_on_slo_violation 분기 키."""
        return self.data_freshness.slo_violated or self.platform_latency.slo_violated


def compute_freshness_seconds(api_ts: datetime, gold_ts: datetime) -> int:
    """`gold_arrival_ts - api_response_ts(tm)` 를 초 단위 정수로 반환.

    음수 (clock skew / out-of-order) 는 0 으로 clamp.
    """
    delta = (gold_ts - api_ts).total_seconds()
    return max(0, int(delta))


def compute_platform_latency_seconds(source_ts: datetime, gold_ts: datetime) -> int:
    """`gold_arrival_ts - source_ts` 를 초 단위 정수로 반환.

    Path B 결정: 현재 `source_ts` = `silver_arrival_ts` (silver Iceberg 적재 시각,
    bronze→silver Flink job 의 `CURRENT_TIMESTAMP`). Phase 1B/2 의 silver schema
    정정 (kafka_ts ADD COLUMN) 시점에 `source_ts` = `kafka_ts METADATA` 로 확장 가능
    (signature 그대로 reuse).

    음수 (clock skew) 는 0 clamp.
    """
    delta = (gold_ts - source_ts).total_seconds()
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


def summarize_one(
    name: str,
    samples: Sequence[int],
    threshold_seconds: int,
) -> MetricSummary:
    """Sample list → MetricSummary. 빈 입력은 모두 0 / not violated."""
    if not samples:
        return MetricSummary(
            name=name,
            threshold_seconds=threshold_seconds,
            count=0,
            p50_seconds=0,
            p95_seconds=0,
            p99_seconds=0,
            max_seconds=0,
            slo_violated=False,
        )
    s = sorted(samples)
    p95 = _percentile(s, 0.95)
    return MetricSummary(
        name=name,
        threshold_seconds=threshold_seconds,
        count=len(s),
        p50_seconds=_percentile(s, 0.50),
        p95_seconds=p95,
        p99_seconds=_percentile(s, 0.99),
        max_seconds=int(s[-1]),
        slo_violated=p95 > threshold_seconds,
    )


def summarize_dual(
    freshness_samples: Sequence[int],
    latency_samples: Sequence[int],
) -> SLOReport:
    """두 가지 sample → SLOReport. 각각 독립 summarize."""
    return SLOReport(
        data_freshness=summarize_one(
            "data_freshness", freshness_samples, SLO_DATA_FRESHNESS_SECONDS
        ),
        platform_latency=summarize_one(
            "platform_latency", latency_samples, SLO_PLATFORM_LATENCY_SECONDS
        ),
    )


def fetch_dual_samples_from_gold() -> tuple[list[int], list[int]]:
    """`gold.fact_hotspot_congestion_5min` 최근 24h row 의 두 가지 lag list.

    - freshness = `gold_arrival_ts - last_api_response_ts` (= API tm)
    - platform_latency = `gold_arrival_ts - last_silver_arrival_ts` (= Kafka broker ts)

    `last_silver_arrival_ts` 가 NULL 인 row (Day 10 PR α schema migration 이전 적재) 는
    platform_latency 측정에서 제외 (`IS NOT NULL` filter). freshness 는
    동일 row 도 측정 가능 (`last_api_response_ts` 는 Day 4 부터 존재).
    """
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
        return [], []

    cutoff = (datetime.now(UTC) - timedelta(hours=24)).replace(tzinfo=None)
    with closing(duckdb.connect()) as con:
        configure_duckdb(con)
        # `union_by_name = true` — Iceberg ALTER ADD COLUMN (Day 10 PR α 의
        # `last_silver_arrival_ts`) 직후 backward read 호환. 일부 parquet 에만 새
        # 컬럼이 있고 다른 parquet 에는 없는 상황 = DuckDB 가 union schema
        # 생성 + missing 컬럼 NULL 채움.
        freshness_rows = con.execute(
            """
            SELECT date_diff('second', last_api_response_ts, gold_arrival_ts) AS sec
            FROM read_parquet(?, hive_partitioning = true, union_by_name = true)
            WHERE last_api_response_ts IS NOT NULL
              AND gold_arrival_ts IS NOT NULL
              AND gold_arrival_ts >= ?
            """,
            [file_paths, cutoff],
        ).fetchall()
        try:
            latency_rows = con.execute(
                """
                SELECT date_diff('second', last_silver_arrival_ts, gold_arrival_ts) AS sec
                FROM read_parquet(?, hive_partitioning = true, union_by_name = true)
                WHERE last_silver_arrival_ts IS NOT NULL
                  AND gold_arrival_ts IS NOT NULL
                  AND gold_arrival_ts >= ?
                """,
                [file_paths, cutoff],
            ).fetchall()
        except duckdb.BinderException:
            # `last_silver_arrival_ts` 가 어느 parquet 에도 아직 없음 (Day 10 PR α
            # migration 직후, silver→gold sink 의 첫 5min tumbling flush 전).
            # graceful degrade — 새 parquet 적재 후 자동 복구.
            log.info(
                "last_silver_arrival_ts not yet in any parquet file — platform_latency samples=[]"
            )
            latency_rows = []
    freshness = [max(0, int(r[0])) for r in freshness_rows if r[0] is not None]
    latency = [max(0, int(r[0])) for r in latency_rows if r[0] is not None]
    return freshness, latency


def _print_summary(s: MetricSummary) -> None:
    print(f"== {s.name} ==")
    print(f"count          : {s.count}")
    print(f"p50 seconds    : {s.p50_seconds}")
    print(f"p95 seconds    : {s.p95_seconds}  (threshold: {s.threshold_seconds})")
    print(f"p99 seconds    : {s.p99_seconds}")
    print(f"max seconds    : {s.max_seconds}")
    print(f"SLO violated   : {s.slo_violated}")


def main() -> None:
    freshness, latency = fetch_dual_samples_from_gold()
    report = summarize_dual(freshness, latency)
    _print_summary(report.data_freshness)
    print()
    _print_summary(report.platform_latency)
    print()
    print("== Overall ==")
    print(f"any_violated   : {report.any_violated}")


if __name__ == "__main__":
    main()
