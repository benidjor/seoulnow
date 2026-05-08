"""Day 4 Task 4.2 — 데이터 신선도 SLO 단위 테스트 (TDD).

`compute_freshness_seconds` / `summarize` / `FreshnessReport` 의 동작을 고정.
실 데이터 fetch (`fetch_samples_from_gold`) 는 Iceberg 의존이라 별도 통합 실행
(`python -m flink_jobs.slo_metrics`) 에서 검증. 본 파일은 순수 함수만.

NOTE — 10개 샘플 fixture 의 p95 expected = 393.
plan(phase-1a-week-1.md L2438) 원본은 414 였으나, `(n-1)*p` linear 보간을 적용한
canonical 값은 392.999... 이며 round 처리 시 393.
414 는 같은 fixture 의 p99 truncation 값(414.6) 과 일치하므로 plan author 의
hand-calc 오류로 판단 (`numpy.percentile([...], 95)` 도 393 반환).
구현은 `int(round(...))` 로 부동소수점 epsilon 안정화.
"""
from __future__ import annotations

from datetime import datetime

from flink_jobs.slo_metrics import FreshnessReport, compute_freshness_seconds, summarize


def test_compute_freshness_seconds_basic() -> None:
    api_ts = datetime(2026, 4, 30, 14, 0, 0)
    gold_ts = datetime(2026, 4, 30, 14, 4, 30)
    assert compute_freshness_seconds(api_ts, gold_ts) == 270


def test_compute_freshness_seconds_negative_clamped_to_zero() -> None:
    api_ts = datetime(2026, 4, 30, 14, 5)
    gold_ts = datetime(2026, 4, 30, 14, 0)
    assert compute_freshness_seconds(api_ts, gold_ts) == 0


def test_summarize_returns_p50_p95_p99_max() -> None:
    samples = [60, 90, 120, 150, 180, 210, 240, 300, 360, 420]
    rep = summarize(samples)
    assert isinstance(rep, FreshnessReport)
    assert rep.count == 10
    assert rep.p50_seconds == 195
    assert rep.p95_seconds == 393
    assert rep.max_seconds == 420
    assert rep.p95_seconds < 7 * 60


def test_summarize_empty_returns_zeros() -> None:
    rep = summarize([])
    assert rep.count == 0
    assert rep.p50_seconds == 0
    assert rep.slo_violated is False


def test_summarize_marks_slo_violated_when_p95_above_7min() -> None:
    samples = [60, 60, 60, 60, 60, 60, 60, 60, 60, 9999]
    rep = summarize(samples)
    assert rep.slo_violated is True
