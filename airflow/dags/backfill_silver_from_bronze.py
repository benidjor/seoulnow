"""backfill_silver_from_bronze — 시간 partition 별 silver 재처리 DAG.

Day 5 Task 5.7 (Day 5~6 buffer) — Airflow 본진 4 DAG 의 두 번째.

본진 기능 (spec §5-8 표 3행):

- Dynamic Task Mapping — `process_partition.partial(...).expand(partition=hours)`
  로 런타임에 N 개 task 자동 생성 (Airflow 2.3+).
- Params — UI 에서 `start_ts` / `end_ts` / `tables` / `dry_run` 입력.
- `max_active_tis_per_dag=2` — Spark 동시 submit 2개 제한 (Day 9 OOM 방지).
- dry_run 모드 — row count 만 추정, 실제 적재 안 함.
- 멱등 MERGE INTO — Day 9 본격 (재실행해도 결과 동일).
- schedule=None — 수동 trigger 전용.

Plan 대비 변경:

- Spark job 본문 = placeholder (Day 9 Task 9.2 진입 시점에 본격
  `spark/jobs/backfill_silver_partition.py` 본문 + 멱등 MERGE INTO).
- `spark_submit` helper 도 Day 5 시점 echo only.
- TDD pytest 미작성 — host venv 의 apache-airflow 미설치 (PR γ Task 5.6 동일
  패턴). 검증 = `airflow dags list-import-errors` 0건 + `tasks list --tree`.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.models.param import Param

from common.callbacks import send_discord_alert
from common.spark_submit import submit_spark_backfill


@dag(
    dag_id="backfill_silver_from_bronze",
    description="시간 partition 별 silver 재처리 (Dynamic Task Mapping + 멱등 MERGE INTO, Day 5 Task 5.7)",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["airflow", "day5", "task5.7", "buffer"],
    default_args={
        "owner": "data-platform",
        "on_failure_callback": send_discord_alert,
    },
    params={
        "start_ts": Param("2026-05-09T00:00:00", type="string"),
        "end_ts": Param("2026-05-09T03:00:00", type="string"),
        "tables": Param(["silver.hotspot_congestion"], type="array"),
        "dry_run": Param(True, type="boolean"),
    },
)
def backfill_silver_from_bronze():
    # 변수명 `params` 회피 — Airflow 의 task context reserved 키워드 (partial
    # 호출 시 ValueError 발생). DAG-level Params 객체와의 의미 충돌도 회피.
    @task
    def validate_params(**context: Any) -> dict:
        cfg = context["params"]
        assert cfg["start_ts"] < cfg["end_ts"], "start_ts < end_ts 필요"
        assert len(cfg["tables"]) > 0, "tables 1개 이상"
        return cfg

    @task
    def generate_hourly_partitions(cfg: dict) -> list[str]:
        start = datetime.fromisoformat(cfg["start_ts"])
        end = datetime.fromisoformat(cfg["end_ts"])
        partitions: list[str] = []
        cur = start
        while cur < end:
            partitions.append(cur.strftime("%Y-%m-%dT%H"))
            cur += timedelta(hours=1)
        return partitions

    @task(max_active_tis_per_dag=2)
    def process_partition(partition: str, cfg: dict) -> dict:
        # Day 5 placeholder. Day 9 Task 9.2 = spark-submit 실 호출.
        results = []
        for table in cfg["tables"]:
            r = submit_spark_backfill(partition, table, cfg["dry_run"])
            results.append(r)
        return {"partition": partition, "tables": results}

    @task
    def verify_silver_row_count(processed: list[dict]) -> dict:
        # Day 5 placeholder. Day 9 = silver row count 검증 (전후 비교).
        return {"processed_partitions": len(processed)}

    @task
    def post_backfill_summary(summary: dict) -> None:
        # Day 5 placeholder. Day 9 = Discord webhook 발신.
        print(f"[Day 5 placeholder] backfill summary: {summary}")

    cfg = validate_params()
    partitions = generate_hourly_partitions(cfg)
    processed = process_partition.partial(cfg=cfg).expand(partition=partitions)
    summary = verify_silver_row_count(processed)
    post_backfill_summary(summary)


backfill_silver_from_bronze()
