"""dbt_full_run — staging → marts 순차 실행 + tests + Discord on_failure callback.

Day 5 Task 5.6 — Airflow 본진 4 DAG 의 첫 번째 (`dbt_full_run`).

본진 기능 (spec §5-8 표 1행):

- TaskGroup `staging` / `marts` — UI 시각 분리.
- Task 의존성 — `staging` 그룹 → `marts` 그룹. staging test 실패 시
  marts 자동 skip (Airflow 의 default trigger_rule = all_success).
- retry policy — retries=2, exponential backoff, retry_delay 5분.
- SLA — 30분 (default_args 안). 30분 초과 시 SLA miss 자동 기록.
- on_failure_callback — Discord webhook 발신 (env 빈 값이면 stdout fallback).
- schedule — `0 2 * * *` (매일 02:00 KST, streaming peak 회피).

Plan 대비 변경 (Task 5.6):

- `dbt_seed` task 제거 — 본 PR β 의 dbt project 에 seed 없음.
- `dbt_docs_generate` / `upload_docs` 제거 — Day 10 dbt-docs 진입 시점 추가.
- dbt CLI 호출 = `${DBT_VENV_BIN}/dbt` (Task 5.5 의 별도 venv,
  protobuf 충돌 회피).
- `DBT_PROFILES_DIR=/opt/airflow/dbt/seoul` (host 의 profiles.yml 마운트).
- Day 6 hotfix: PYTHONPATH 추가 (dbt python model 의 flink_jobs /
  platform_common import 경로 보강 — `./src` ro mount 에 대응).
- Day 6 hotfix follow-up: `LAKEKEEPER_URL` / `MINIO_ENDPOINT` 도
  `dbt_env` dict 에 명시 추가. `BashOperator(env=dict, append_env=True)`
  의 os.environ inherit 가 dbt-duckdb 의 python model subprocess 까지
  닿지 않아 `platform_common.Settings` 가 default `localhost:*` 사용 →
  Connection refused 발생 (PR α0 머지 후 attempt 3 재현). dict 에 직접
  set 한 키 (`PYTHONPATH`) 는 정상 transmitted 인 점이 결정 증거.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from common.callbacks import send_discord_alert

DBT_DIR = "/opt/airflow/dbt/seoul"
DBT_BIN = "/opt/airflow/dbt-venv/bin/dbt"

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_exponential_backoff": True,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=30),
    "on_failure_callback": send_discord_alert,
}

dbt_env = {
    "DBT_PROFILES_DIR": DBT_DIR,
    # Day 6 hotfix — `stg_hotspot_silver` (dbt python model) 가
    # `flink_jobs.lib.duckdb_iceberg` / `platform_common.config` 를 import.
    # docker-compose 에서 `./src:/opt/airflow/repo-src:ro` mount, 여기서
    # path 추가.
    "PYTHONPATH": "/opt/airflow/repo-src",
    # Day 6 hotfix follow-up — `BashOperator(append_env=True)` 가
    # dbt-duckdb python model subprocess 까지 inherit 안 됨. dict 에
    # 명시 set 한 키만 transmitted (PYTHONPATH 가 정상 작동한 점이
    # 증거). `platform_common.Settings` 의 docker DNS override.
    "LAKEKEEPER_URL": "http://lakekeeper:8181",
    "MINIO_ENDPOINT": "http://minio:9000",
}


with DAG(
    dag_id="dbt_full_run",
    description="dbt staging → marts 순차 실행 + tests + on_failure callback (Day 5 Task 5.6)",
    start_date=datetime(2026, 5, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["dbt", "day5", "task5.6"],
) as dag:

    with TaskGroup("staging") as staging:
        dbt_run_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=f"cd {DBT_DIR} && {DBT_BIN} run --select staging",
            env=dbt_env,
            append_env=True,
        )
        dbt_test_staging = BashOperator(
            task_id="dbt_test_staging",
            bash_command=f"cd {DBT_DIR} && {DBT_BIN} test --select staging",
            env=dbt_env,
            append_env=True,
        )
        dbt_run_staging >> dbt_test_staging

    with TaskGroup("marts") as marts:
        dbt_run_marts = BashOperator(
            task_id="dbt_run_marts",
            bash_command=f"cd {DBT_DIR} && {DBT_BIN} run --select marts",
            env=dbt_env,
            append_env=True,
        )
        dbt_test_marts = BashOperator(
            task_id="dbt_test_marts",
            bash_command=f"cd {DBT_DIR} && {DBT_BIN} test --select marts",
            env=dbt_env,
            append_env=True,
        )
        dbt_run_marts >> dbt_test_marts

    staging >> marts
