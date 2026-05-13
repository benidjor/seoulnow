"""slo_daily_report — Day 10 PR α (Airflow 본진 4 DAG 라인업 완성, 4번째).

두 종 SLO 일일 리포트 + BranchPythonOperator + Discord 알림. spec §6-2 정정
(data freshness P95 < 45m / platform latency P95 < 7m) 의 운영 진입점.

본진 기능 (spec §5-8 표 4행):

- **BranchPythonOperator**: `branch_on_slo_violation` 이 XCom 의
  `any_violated` 를 읽어 `"send_alert"` / `"skip_alert"` 분기.
- **XCom 흐름**: `collect_slo_metrics` (BashOperator stdout JSON →
  do_xcom_push) → `generate_report` (pull → Jinja-less inline format →
  markdown 파일) → `branch_on_slo_violation` (pull → 분기) → `send_alert`
  (pull → Discord 메시지).
- **on_failure_callback**: 리포트 생성 자체 실패도 Discord alert.
- **schedule `0 9 * * *`**: 매일 09:00 KST, 어제 하루 집계.
- **Option B (BashOperator + dbt-venv subprocess)**: Airflow 기본 venv 의
  duckdb / pyiceberg 미설치 회피. Day 9 PR γ 의 patterns SoT 그대로 reuse.

SLO 임계값 상수 (spec §6-2 SoT):

- `DATA_FRESHNESS_THRESHOLD_SECONDS = 45 * 60` — API tm → Gold P95
- `PLATFORM_LATENCY_THRESHOLD_SECONDS = 7 * 60` — Kafka → Gold P95

Test SoT: `tests/unit/airflow/test_slo_daily_report_dag.py` (7 case).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from common.callbacks import send_discord_alert, send_slo_alert

from airflow import DAG

# spec §6-2 SoT — Test (test_slo_thresholds_match_spec) 가 본 상수 일치 검증.
DATA_FRESHNESS_THRESHOLD_SECONDS = 45 * 60
PLATFORM_LATENCY_THRESHOLD_SECONDS = 7 * 60

# Option B — Day 9 PR γ 의 iceberg_maintenance.py 패턴 그대로 reuse.
DBT_VENV_PYTHON = "/opt/airflow/dbt-venv/bin/python"
SLO_QUERY_SCRIPT = "/opt/airflow/dags/common/slo_query.py"
REPORT_DIR = Path("/opt/airflow/data/slo_reports")

slo_env = {
    "PYTHONPATH": "/opt/airflow/repo-src",
    "LAKEKEEPER_URL": "http://lakekeeper:8181",
    "MINIO_ENDPOINT": "http://minio:9000",
}


def _parse_xcom_report(raw: Any) -> dict[str, Any]:
    """BashOperator stdout JSON string + PythonOperator return dict 둘 다 정합."""
    if isinstance(raw, str):
        return json.loads(raw)
    return raw or {}


def branch_on_slo_violation(task_instance: Any = None, **_: Any) -> str:
    """BranchPythonOperator callable.

    XCom 의 `collect_slo_metrics` payload 에서 `any_violated` 를 읽어 분기
    task_id 반환. Test 가 본 함수의 결과를 직접 검증 (Mock TI).
    """
    raw = task_instance.xcom_pull(task_ids="collect_slo_metrics")
    report = _parse_xcom_report(raw)
    if report.get("any_violated"):
        return "send_alert"
    return "skip_alert"


def generate_report(task_instance: Any = None, ds: str | None = None, **_: Any) -> str:
    """XCom 의 두 종 SLO 메트릭 → markdown 파일 작성. 파일 path return.

    Phase 1A 한정 — markdown 만 보관. Phase 2 에서 `archive.fact_slo_daily`
    적재 (시계열 SLO 추세 데이터셋, Superset source).
    """
    raw = task_instance.xcom_pull(task_ids="collect_slo_metrics")
    report = _parse_xcom_report(raw)
    df = report.get("data_freshness", {})
    pl = report.get("platform_latency", {})

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    file_path = REPORT_DIR / f"slo-{ds or 'unknown'}.md"
    file_path.write_text(
        f"# SLO Daily Report — {ds}\n\n"
        f"## (α) Data Freshness (API tm → Gold)\n"
        f"- count: {df.get('count')}\n"
        f"- p50 / p95 / p99 / max: "
        f"{df.get('p50_seconds')}s / {df.get('p95_seconds')}s / "
        f"{df.get('p99_seconds')}s / {df.get('max_seconds')}s\n"
        f"- threshold: {df.get('threshold_seconds')}s\n"
        f"- violated: {df.get('slo_violated')}\n\n"
        f"## (β) Platform Latency (Silver → Gold)\n"
        f"- count: {pl.get('count')}\n"
        f"- p50 / p95 / p99 / max: "
        f"{pl.get('p50_seconds')}s / {pl.get('p95_seconds')}s / "
        f"{pl.get('p99_seconds')}s / {pl.get('max_seconds')}s\n"
        f"- threshold: {pl.get('threshold_seconds')}s\n"
        f"- violated: {pl.get('slo_violated')}\n\n"
        f"## Overall\n"
        f"- any_violated: {report.get('any_violated')}\n",
        encoding="utf-8",
    )
    return str(file_path)


default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=30),
    "on_failure_callback": send_discord_alert,
}


with DAG(
    dag_id="slo_daily_report",
    description="두 종 SLO (data freshness + platform latency) 일일 리포트 + Discord 알림",
    start_date=datetime(2026, 5, 1),
    schedule="0 9 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["airflow", "day10", "task10.3", "slo"],
) as dag:
    collect_slo_metrics = BashOperator(
        task_id="collect_slo_metrics",
        bash_command=f"{DBT_VENV_PYTHON} {SLO_QUERY_SCRIPT}",
        env=slo_env,
        append_env=True,
        do_xcom_push=True,
    )

    generate_report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_on_slo_violation",
        python_callable=branch_on_slo_violation,
    )

    send_alert_task = PythonOperator(
        task_id="send_alert",
        python_callable=send_slo_alert,
    )

    skip_alert_task = EmptyOperator(task_id="skip_alert")

    archive_report_task = EmptyOperator(
        task_id="archive_report",
        trigger_rule="none_failed_min_one_success",
    )

    collect_slo_metrics >> generate_report_task >> branch_task
    branch_task >> [send_alert_task, skip_alert_task] >> archive_report_task
