"""iceberg_maintenance — Day 9 본격 활성 (rewrite_data_files + Discord 알림).

Day 5 Task 5.8 buffer 의 echo placeholder 7개 → 본격 활성 (6개로 reduction):

- snapshot_metrics_before/after = BashOperator + dbt-venv subprocess
  (Option B 채택, commit 4) — dbt_full_run.py 의 BashOperator + dbt-venv 패턴
  reuse. Airflow 기본 venv 의 duckdb / pyiceberg 미설치 회피.
  **Task 11.1-A 변경**: silver + gold 2 table 동시 측정 (SILVER_TABLE GOLD_TABLE).
- rewrite_silver_hotspot_congestion = BashOperator + docker run scp/spark +
  spark-submit compaction_silver.py
- **rewrite_gold_fact_hotspot_congestion (Task 11.1-A 신규)** = BashOperator +
  docker run scp/spark + spark-submit compaction_gold.py (gold rewrite child 복원).
- post_compaction_report = PythonOperator + XCom pull + send_compaction_report
  (Discord webhook + stdout fallback)
- expire_snapshots = **실 spark-submit 활성화 (Day 9 A3 해제, Task 11.1-A)**
  spark-submit expire_snapshots.py (older_than 7d)
- remove_orphan_files = echo placeholder 유지 (Phase 2 본격)

본진 기능 (spec §5-8 표 2행):

- 병렬 실행 — TaskGroup `rewrite` (silver + gold 2 child 병렬, P1B 후
  rewrite_user_event 추가 시 3-way).
- `max_active_tis_per_dag=3` — Spark concurrent submit 제한.
- XCom — before/after 메트릭 (files / bytes / snapshots, JSON string).
- on_failure_callback — `send_discord_alert` (Discord webhook + stdout
  fallback).
- SLA 1시간 — 메모리 ceiling 위협 자동 감지.
- schedule `0 3 * * *` — 매일 03:00 KST (streaming peak 회피).

PR α (#53) + PR β (#54) deviation reuse (변경 0건):

- 9.1-A: warehouse=seoul (spark-defaults.conf).
- 9.1-B: iceberg-aws-bundle (Dockerfile).
- 9.1-C: extraClassPath 절대 경로 (spark-defaults.conf).
- 9.1-D: `docker run --rm` 자동 cleanup (정공 명령 정신 보존).
- 9.2-A 확장: procedure call argument 2-part `silver.hotspot_congestion`.

사용자 결정 사항 (Day 9 PR γ):

- A1: `rewrite_dim_place` task 제거 (P1B 후 활성화 의무, Plan SoT line 2316).
- A2: task_id rename `rewrite_fact_hotspot_congestion_5min` →
  `rewrite_silver_hotspot_congestion` (PR β 의 compaction_silver.py =
  silver.hotspot_congestion 대상과 정합).
- A3: `expire_snapshots` + `remove_orphan_files` placeholder 유지 (Day 10
  또는 Phase 2 본격).
- A4: docker socket mount + `docker run --rm` (Airflow image rebuild 회피).

commit 4 deviation (Option B 채택 SoT):

- 이전 commit 2 = PythonOperator + in-process `_capture_metrics` callable.
  Airflow 기본 venv 에 duckdb / pyiceberg 미설치 → manual trigger 시
  `ModuleNotFoundError` 발생.
- 신규 commit 4 = BashOperator + dbt-venv subprocess (capture_metrics.py).
  dbt_full_run.py 의 BashOperator + dbt-venv 패턴 SoT 일치. cloudpickle 의존성
  부담 + Dockerfile rebuild 부담 둘 다 회피.

보안 limitation:

- docker socket mount = Airflow 컨테이너가 host docker daemon 의 root 권한
  직접 사용. Phase 1A 데모 한정 (single-user laptop, public 공개 없음).
- Phase 2 Oracle Cloud 배포 시 Spark on Kubernetes / SparkSubmitOperator +
  Livy 또는 SSHOperator 로 재설계 의무.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from common.callbacks import send_compaction_report, send_discord_alert

from airflow import DAG

SILVER_TABLE = "silver.hotspot_congestion"
GOLD_TABLE = "gold.fact_hotspot_congestion_5min"
SPARK_IMAGE = "scp/spark:3.5.3-iceberg"
SPARK_NETWORK = "scp_default"  # compose project name `scp` SoT (docker-compose.yml L1)

# Option B (commit 4) — BashOperator + dbt-venv subprocess.
# dbt_full_run.py 의 Day 5 본진 정공 패턴 SoT 일치.
DBT_VENV_PYTHON = "/opt/airflow/dbt-venv/bin/python"
CAPTURE_METRICS_SCRIPT = "/opt/airflow/dags/common/capture_metrics.py"

metrics_env = {
    # dbt_full_run.py 의 Day 6 hotfix follow-up 패턴 reuse — dict 에 명시 set
    # 한 키만 subprocess 에 transmitted (append_env=True 가 base env merge).
    "PYTHONPATH": "/opt/airflow/repo-src",
    "LAKEKEEPER_URL": "http://lakekeeper:8181",
    "MINIO_ENDPOINT": "http://minio:9000",
}


default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=1),
    "on_failure_callback": send_discord_alert,
}


with DAG(
    dag_id="iceberg_maintenance",
    description="Iceberg compaction + Discord 알림 (Day 9 Task 9.3 본격 활성)",
    start_date=datetime(2026, 5, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["airflow", "day9", "task9.3", "iceberg-maintenance"],
) as dag:
    snapshot_metrics_before = BashOperator(
        task_id="snapshot_metrics_before",
        bash_command=f"{DBT_VENV_PYTHON} {CAPTURE_METRICS_SCRIPT} {SILVER_TABLE} {GOLD_TABLE}",
        env=metrics_env,
        append_env=True,
        do_xcom_push=True,
    )

    with TaskGroup("rewrite") as rewrite:
        # docker run --rm (compose plugin 부재 우회). --rm 자동 cleanup.
        # PR α (#53) 의 9.1-D 정공 명령 정신 보존.
        # PROJECT_ROOT = docker-compose.yml 의 airflow-common environment 에서
        # ${PWD} default 로 host CWD = 프로젝트 루트 주입 (Step 1).
        rewrite_silver_hotspot_congestion = BashOperator(
            task_id="rewrite_silver_hotspot_congestion",
            bash_command=(
                "set -e\n"
                "docker run --rm "
                f"--network {SPARK_NETWORK} "
                "-v ${PROJECT_ROOT}/infra/spark/conf:/opt/spark/conf:ro "
                "-v ${PROJECT_ROOT}/infra/spark/jobs:/workspace/jobs:ro "
                "-e AWS_ACCESS_KEY_ID=minioadmin "
                "-e AWS_SECRET_ACCESS_KEY=minioadmin "
                "-e AWS_REGION=us-east-1 "
                f"{SPARK_IMAGE} "
                "/opt/spark/bin/spark-submit /workspace/jobs/compaction_silver.py\n"
            ),
            max_active_tis_per_dag=3,
        )

        rewrite_gold_fact_hotspot_congestion = BashOperator(
            task_id="rewrite_gold_fact_hotspot_congestion",
            bash_command=(
                "set -e\n"
                "docker run --rm "
                f"--network {SPARK_NETWORK} "
                "-v ${PROJECT_ROOT}/infra/spark/conf:/opt/spark/conf:ro "
                "-v ${PROJECT_ROOT}/infra/spark/jobs:/workspace/jobs:ro "
                "-e AWS_ACCESS_KEY_ID=minioadmin "
                "-e AWS_SECRET_ACCESS_KEY=minioadmin "
                "-e AWS_REGION=us-east-1 "
                f"{SPARK_IMAGE} "
                "/opt/spark/bin/spark-submit /workspace/jobs/compaction_gold.py\n"
            ),
            max_active_tis_per_dag=3,
        )

    expire_snapshots = BashOperator(
        task_id="expire_snapshots",
        bash_command=(
            "set -e\n"
            "docker run --rm "
            f"--network {SPARK_NETWORK} "
            "-v ${PROJECT_ROOT}/infra/spark/conf:/opt/spark/conf:ro "
            "-v ${PROJECT_ROOT}/infra/spark/jobs:/workspace/jobs:ro "
            "-e AWS_ACCESS_KEY_ID=minioadmin "
            "-e AWS_SECRET_ACCESS_KEY=minioadmin "
            "-e AWS_REGION=us-east-1 "
            f"{SPARK_IMAGE} "
            "/opt/spark/bin/spark-submit /workspace/jobs/expire_snapshots.py\n"
        ),
        max_active_tis_per_dag=3,
    )

    remove_orphan_files = BashOperator(
        task_id="remove_orphan_files",
        bash_command=(
            "echo '[Day 9 placeholder] remove_orphan_files older_than 3d "
            "(Day 10 또는 Phase 2 본격 활성)'"
        ),
    )

    snapshot_metrics_after = BashOperator(
        task_id="snapshot_metrics_after",
        bash_command=f"{DBT_VENV_PYTHON} {CAPTURE_METRICS_SCRIPT} {SILVER_TABLE} {GOLD_TABLE}",
        env=metrics_env,
        append_env=True,
        do_xcom_push=True,
    )

    post_compaction_report = PythonOperator(
        task_id="post_compaction_report",
        python_callable=send_compaction_report,
    )

    (
        snapshot_metrics_before
        >> rewrite
        >> expire_snapshots
        >> remove_orphan_files
        >> snapshot_metrics_after
        >> post_compaction_report
    )
