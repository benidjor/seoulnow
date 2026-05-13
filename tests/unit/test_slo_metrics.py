"""Day 4 Task 4.2 + Day 10 PR α — 두 가지 SLO 단위 테스트 (TDD).

순수 함수만 다룬다:
- `compute_freshness_seconds` / `compute_platform_latency_seconds` (Day 10 신규)
- `summarize_one` (Day 4 `summarize` → Day 10 일반화)
- `summarize_dual` / `SLOReport.any_violated` (Day 10 신규)

실 데이터 fetch (`fetch_dual_samples_from_gold`) 는 Iceberg 의존이라 별도 통합
실행 (`python -m flink_jobs.slo_metrics`) 에서 검증.

NOTE — 10개 샘플 fixture 의 p95 expected = 393.
plan(phase-1a-week-1.md L2438) 원본은 414 였으나, `(n-1)*p` linear 보간을 적용한
canonical 값은 392.999... 이며 round 처리 시 393. 414 는 같은 fixture 의 p99
truncation 값(414.6) 과 일치하므로 plan author 의 hand-calc 오류로 판단
(`numpy.percentile([...], 95)` 도 393 반환). 구현은 `int(round(...))` 로
부동소수점 epsilon 안정화.
"""
from __future__ import annotations

from datetime import datetime

from flink_jobs.slo_metrics import (
    SLO_DATA_FRESHNESS_SECONDS,
    SLO_PLATFORM_LATENCY_SECONDS,
    MetricSummary,
    SLOReport,
    compute_freshness_seconds,
    compute_platform_latency_seconds,
    summarize_dual,
    summarize_one,
)


def test_compute_freshness_seconds_basic() -> None:
    api_ts = datetime(2026, 4, 30, 14, 0, 0)
    gold_ts = datetime(2026, 4, 30, 14, 4, 30)
    assert compute_freshness_seconds(api_ts, gold_ts) == 270


def test_compute_freshness_seconds_negative_clamped_to_zero() -> None:
    api_ts = datetime(2026, 4, 30, 14, 5)
    gold_ts = datetime(2026, 4, 30, 14, 0)
    assert compute_freshness_seconds(api_ts, gold_ts) == 0


def test_compute_platform_latency_seconds_basic() -> None:
    source_ts = datetime(2026, 5, 13, 14, 0, 0)
    gold_ts = datetime(2026, 5, 13, 14, 0, 30)
    assert compute_platform_latency_seconds(source_ts, gold_ts) == 30


def test_compute_platform_latency_seconds_negative_clamped_to_zero() -> None:
    source_ts = datetime(2026, 5, 13, 14, 1)
    gold_ts = datetime(2026, 5, 13, 14, 0)
    assert compute_platform_latency_seconds(source_ts, gold_ts) == 0


def test_slo_threshold_constants() -> None:
    """spec §6-2 정정 — data freshness P95 < 45m / platform latency P95 < 7m."""
    assert SLO_DATA_FRESHNESS_SECONDS == 45 * 60
    assert SLO_PLATFORM_LATENCY_SECONDS == 7 * 60


def test_summarize_one_returns_p50_p95_p99_max() -> None:
    samples = [60, 90, 120, 150, 180, 210, 240, 300, 360, 420]
    rep = summarize_one("test_metric", samples, threshold_seconds=7 * 60)
    assert isinstance(rep, MetricSummary)
    assert rep.name == "test_metric"
    assert rep.threshold_seconds == 7 * 60
    assert rep.count == 10
    assert rep.p50_seconds == 195
    assert rep.p95_seconds == 393
    assert rep.max_seconds == 420
    assert rep.slo_violated is False


def test_summarize_one_empty_returns_zeros() -> None:
    rep = summarize_one("test_metric", [], threshold_seconds=7 * 60)
    assert rep.count == 0
    assert rep.p50_seconds == 0
    assert rep.slo_violated is False


def test_summarize_one_marks_slo_violated_when_p95_above_threshold() -> None:
    samples = [60, 60, 60, 60, 60, 60, 60, 60, 60, 9999]
    rep = summarize_one("test_metric", samples, threshold_seconds=7 * 60)
    assert rep.slo_violated is True


def test_summarize_dual_data_freshness_only_violated() -> None:
    """data freshness 만 위반 → any_violated True."""
    freshness = [3000] * 10  # 50분 = 45분 threshold 초과
    latency = [60] * 10  # 1분 = 7분 threshold 안
    report = summarize_dual(freshness, latency)
    assert report.data_freshness.slo_violated is True
    assert report.platform_latency.slo_violated is False
    assert report.any_violated is True


def test_summarize_dual_platform_latency_only_violated() -> None:
    """platform latency 만 위반 → any_violated True."""
    freshness = [600] * 10  # 10분 = 45분 안
    latency = [500] * 10  # 8.3분 = 7분 threshold 초과
    report = summarize_dual(freshness, latency)
    assert report.data_freshness.slo_violated is False
    assert report.platform_latency.slo_violated is True
    assert report.any_violated is True


def test_summarize_dual_both_within_slo_any_violated_false() -> None:
    """둘 다 SLO 안 → any_violated False (skip_alert 분기)."""
    freshness = [600] * 10  # 10분
    latency = [60] * 10  # 1분
    report = summarize_dual(freshness, latency)
    assert report.data_freshness.slo_violated is False
    assert report.platform_latency.slo_violated is False
    assert report.any_violated is False


def test_summarize_dual_returns_slo_report_with_named_metrics() -> None:
    """SLOReport 의 두 가지 MetricSummary 가 spec §6-2 의 이름 + threshold 가짐."""
    report = summarize_dual([60], [60])
    assert isinstance(report, SLOReport)
    assert report.data_freshness.name == "data_freshness"
    assert report.data_freshness.threshold_seconds == SLO_DATA_FRESHNESS_SECONDS
    assert report.platform_latency.name == "platform_latency"
    assert report.platform_latency.threshold_seconds == SLO_PLATFORM_LATENCY_SECONDS


def test_summarize_dual_empty_samples_both_zero() -> None:
    """빈 입력 → 둘 다 count=0, any_violated False."""
    report = summarize_dual([], [])
    assert report.data_freshness.count == 0
    assert report.platform_latency.count == 0
    assert report.any_violated is False
