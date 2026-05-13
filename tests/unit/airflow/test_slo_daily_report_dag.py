"""slo_daily_report DAG 단위 테스트 (Day 10 PR α).

Plan §Task 10.3 TDD Step 1-7 (6 case + threshold constants = 7 case):

1. DAG 파싱 OK + dag_id 정합 + schedule `0 9 * * *` (매일 09:00 KST).
2. branch_on_slo_violation — data freshness only 위반 → "send_alert".
3. branch_on_slo_violation — platform latency only 위반 → "send_alert".
4. branch_on_slo_violation — 둘 다 안 위반 → "skip_alert".
5. XCom keys consistent — branch_on_slo_violation 의 pull task_ids 가
   "collect_slo_metrics" 와 일치.
6. build_slo_alert_message — payload 에 두 종 메트릭 모두 포함 + 위반 SLO 명시.
7. DAG 의 SLO 임계값 상수가 spec §6-2 SoT (45m / 7m) 와 일치.

host venv 는 airflow 미설치 (pyproject.toml 의 dep 미포함 — Airflow 는 컨테이너
안 base image 가 제공). host pytest 실행 시 자동 skip, Airflow 컨테이너 안
pytest 실행 시 본격 검증. 패턴 SoT = test_iceberg_maintenance_dag.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

try:
    from airflow import DAG as _DAG  # noqa: F401
except ImportError:
    pytest.skip(
        "Apache Airflow 미설치 (host venv) — Airflow 컨테이너 안에서 실행 의무.",
        allow_module_level=True,
    )

ROOT = Path(__file__).parents[3]
DAGS_DIR = ROOT / "airflow" / "dags"
SRC_DIR = ROOT / "src"
sys.path.insert(0, str(DAGS_DIR))
sys.path.insert(0, str(SRC_DIR))


def test_dag_loads():
    """slo_daily_report DAG 파싱 OK + dag_id + schedule = 매일 09:00 KST."""
    from slo_daily_report import dag  # type: ignore

    from airflow import DAG

    assert isinstance(dag, DAG)
    assert dag.dag_id == "slo_daily_report"
    assert dag.schedule_interval == "0 9 * * *"


def test_branch_send_alert_when_data_freshness_violated():
    """data freshness 만 위반 → 'send_alert' 반환."""
    from slo_daily_report import branch_on_slo_violation  # type: ignore

    class _MockTI:
        def xcom_pull(self, task_ids: str):  # noqa: ARG002
            return {
                "data_freshness": {"slo_violated": True, "p95_seconds": 3000},
                "platform_latency": {"slo_violated": False, "p95_seconds": 200},
                "any_violated": True,
            }

    result = branch_on_slo_violation(task_instance=_MockTI())
    assert result == "send_alert"


def test_branch_send_alert_when_platform_latency_violated():
    """platform latency 만 위반 → 'send_alert' 반환."""
    from slo_daily_report import branch_on_slo_violation  # type: ignore

    class _MockTI:
        def xcom_pull(self, task_ids: str):  # noqa: ARG002
            return {
                "data_freshness": {"slo_violated": False, "p95_seconds": 1000},
                "platform_latency": {"slo_violated": True, "p95_seconds": 500},
                "any_violated": True,
            }

    result = branch_on_slo_violation(task_instance=_MockTI())
    assert result == "send_alert"


def test_branch_skip_alert_when_both_within_slo():
    """둘 다 안 위반 → 'skip_alert' 반환."""
    from slo_daily_report import branch_on_slo_violation  # type: ignore

    class _MockTI:
        def xcom_pull(self, task_ids: str):  # noqa: ARG002
            return {
                "data_freshness": {"slo_violated": False, "p95_seconds": 1000},
                "platform_latency": {"slo_violated": False, "p95_seconds": 200},
                "any_violated": False,
            }

    result = branch_on_slo_violation(task_instance=_MockTI())
    assert result == "skip_alert"


def test_xcom_keys_consistent():
    """branch_on_slo_violation 이 xcom_pull 시 'collect_slo_metrics' task_ids 사용."""
    from slo_daily_report import branch_on_slo_violation  # type: ignore

    pulled_task_ids: list[str] = []

    class _MockTI:
        def xcom_pull(self, task_ids: str):
            pulled_task_ids.append(task_ids)
            return {
                "data_freshness": {"slo_violated": False, "p95_seconds": 100},
                "platform_latency": {"slo_violated": False, "p95_seconds": 100},
                "any_violated": False,
            }

    branch_on_slo_violation(task_instance=_MockTI())
    assert pulled_task_ids == ["collect_slo_metrics"]


def test_send_alert_payload_includes_both_metrics():
    """build_slo_alert_message — payload 에 두 종 메트릭 모두 포함 + 위반 SLO 명시."""
    from common.callbacks import build_slo_alert_message  # type: ignore

    report = {
        "data_freshness": {
            "name": "data_freshness",
            "threshold_seconds": 2700,
            "count": 100,
            "p50_seconds": 2500,
            "p95_seconds": 11840,
            "p99_seconds": 30000,
            "slo_violated": True,
        },
        "platform_latency": {
            "name": "platform_latency",
            "threshold_seconds": 420,
            "count": 100,
            "p50_seconds": 60,
            "p95_seconds": 200,
            "p99_seconds": 300,
            "slo_violated": False,
        },
        "any_violated": True,
    }

    msg = build_slo_alert_message(report)
    assert "data_freshness" in msg  # 위반 SLO 명시
    assert "platform_latency" in msg  # 두 메트릭 모두 포함
    assert "11840" in msg  # data_freshness p95
    assert "200" in msg  # platform_latency p95
    assert "VIOLATION" in msg  # 위반 마커


def test_slo_thresholds_match_spec():
    """DAG 의 SLO 임계값이 spec §6-2 SoT (data freshness 45m / platform latency 7m) 와 일치."""
    from slo_daily_report import (  # type: ignore
        DATA_FRESHNESS_THRESHOLD_SECONDS,
        PLATFORM_LATENCY_THRESHOLD_SECONDS,
    )

    assert DATA_FRESHNESS_THRESHOLD_SECONDS == 45 * 60
    assert PLATFORM_LATENCY_THRESHOLD_SECONDS == 7 * 60
