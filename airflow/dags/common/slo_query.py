"""Day 10 PR α — `slo_daily_report` DAG 의 두 종 SLO 측정 entry point.

BashOperator subprocess 안에서 dbt-venv 의 python 으로 호출 (Day 9 PR γ Option B
패턴 reuse — Airflow base image 에 duckdb / pyiceberg 직접 안 깔고 venv 격리).
stdout 마지막 라인 = JSON SLOReport → BashOperator `do_xcom_push=True` 로 자동
XCom push → 후속 task (generate_report / branch_on_slo_violation / send_alert) 가
pull.

본 모듈은 `flink_jobs.slo_metrics` 의 `fetch_dual_samples_from_gold` +
`summarize_dual` 를 호출하는 thin wrapper. 메인 로직은 src/flink_jobs/slo_metrics
가 단일 출처.

Output schema (stdout 마지막 라인, JSON):
    {
      "data_freshness": {
        "name": "data_freshness",
        "threshold_seconds": 2700,
        "count": int,
        "p50_seconds": int,
        "p95_seconds": int,
        "p99_seconds": int,
        "max_seconds": int,
        "slo_violated": bool
      },
      "platform_latency": { ... 동일 ... },
      "any_violated": bool
    }

Run (standalone test):
    JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \\
        uv run --extra flink python airflow/dags/common/slo_query.py
"""
from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

# Airflow scheduler 가 본 module 을 import 할 때 src/ 가 sys.path 에 없음.
# project root 의 src 를 명시 추가 (`airflow/dags/common/slo_query.py` → 3 단계 상위).
PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from flink_jobs.slo_metrics import (  # noqa: E402
    fetch_dual_samples_from_gold,
    summarize_dual,
)


def run() -> dict[str, Any]:
    """두 종 SLO 측정 → dict (JSON serializable). XCom payload schema."""
    freshness, latency = fetch_dual_samples_from_gold()
    report = summarize_dual(freshness, latency)
    return {
        "data_freshness": asdict(report.data_freshness),
        "platform_latency": asdict(report.platform_latency),
        "any_violated": report.any_violated,
    }


def main() -> None:
    payload = run()
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
