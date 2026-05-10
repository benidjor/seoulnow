"""iceberg_maintenance — Iceberg compaction + snapshot expire 골격 DAG.

Day 5 Task 5.8 (Day 5~6 buffer) — Airflow 본진 4 DAG 의 세 번째 (Day 9 본격).

본진 기능 (spec §5-8 표 2행, Day 9 본격 활성):

- 병렬 실행 — TaskGroup `rewrite` 안 (rewrite_fact_hotspot, rewrite_dim_place
  Day 6 CDC 후 활성).
- `max_active_tis_per_dag=3` — Spark concurrent submit 제한.
- XCom — before/after 메트릭 (file_count / total_bytes / snapshot_count).
- on_success_callback — Discord 압축률 보고 (Day 9 시점).
- SLA 1시간 — 메모리 ceiling 위협 자동 감지.
- schedule: `0 3 * * *` (Day 9 진입 시점 활성).

Day 5 시점 = BashOperator echo placeholder. DAG 파싱 / 병렬 구조 / SLA 검증만.
Day 9 Task 9.3 진입 시점에 SparkSubmitOperator + Iceberg `rewrite_data_files`
+ `expire_snapshots` + `remove_orphan_files` 본격.

Plan 대비 변경:

- BashOperator placeholder (Day 9 SparkSubmitOperator 교체).
- TDD pytest 미작성 — host venv 패턴.
- schedule=None (Day 9 진입 시점 "0 3 * * *" 활성).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from common.callbacks import send_discord_alert

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "sla": timedelta(hours=1),
    "on_failure_callback": send_discord_alert,
}


with DAG(
    dag_id="iceberg_maintenance",
    description="Iceberg compaction + snapshot expire 골격 (Day 5 Task 5.8, Day 9 본격)",
    start_date=datetime(2026, 5, 1),
    schedule=None,  # Day 9 진입 시점 "0 3 * * *" 활성
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["airflow", "day5", "task5.8", "buffer"],
) as dag:

    snapshot_metrics_before = BashOperator(
        task_id="snapshot_metrics_before",
        bash_command="echo '[Day 5 placeholder] snapshot metrics before (Day 9 = pyiceberg lookup + XCom push)'",
    )

    with TaskGroup("rewrite") as rewrite:
        rewrite_fact_hotspot = BashOperator(
            task_id="rewrite_fact_hotspot_congestion_5min",
            bash_command="echo '[Day 5 placeholder] rewrite_data_files fact_hotspot_congestion_5min (Day 9 = Spark CALL)'",
            max_active_tis_per_dag=3,
        )
        rewrite_dim_place = BashOperator(
            task_id="rewrite_dim_place",
            bash_command="echo '[Day 5 placeholder] rewrite_data_files dim_place (Day 6 CDC 후 활성)'",
            max_active_tis_per_dag=3,
        )

    expire_snapshots = BashOperator(
        task_id="expire_snapshots",
        bash_command="echo '[Day 5 placeholder] expire_snapshots older_than 7d (Day 9 = Spark CALL)'",
    )

    remove_orphan_files = BashOperator(
        task_id="remove_orphan_files",
        bash_command="echo '[Day 5 placeholder] remove_orphan_files older_than 3d (Day 9 = Spark CALL)'",
    )

    snapshot_metrics_after = BashOperator(
        task_id="snapshot_metrics_after",
        bash_command="echo '[Day 5 placeholder] snapshot metrics after (Day 9 = pyiceberg lookup + XCom push)'",
    )

    post_compaction_report = BashOperator(
        task_id="post_compaction_report",
        bash_command="echo '[Day 5 placeholder] compaction report (Day 9 = XCom pull → Discord webhook)'",
    )

    snapshot_metrics_before >> rewrite >> expire_snapshots >> remove_orphan_files >> snapshot_metrics_after >> post_compaction_report
