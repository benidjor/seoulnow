"""Iceberg table 메트릭 측정 helper — Airflow BashOperator + dbt-venv subprocess 호출 용.

Day 9 PR γ commit 4 — Option B 채택. PythonOperator (in-process) → BashOperator
+ dbt-venv subprocess 패턴 전환 (dbt_full_run.py SoT 일치).

배경:
- 이전 commit 2 의 PythonOperator + in-process `_capture_metrics` callable 호출은
  Airflow 기본 venv (`/home/airflow/.local/lib/python3.11/site-packages`) 에서
  실행. 다만 거기에는 duckdb / pyiceberg 미설치 → `ModuleNotFoundError` fail.
- dbt-venv (`/opt/airflow/dbt-venv/`) 안에는 duckdb + pyiceberg 0.11.1 모두
  설치되어 있음. dbt_full_run.py 가 BashOperator + dbt-venv subprocess 패턴으로
  같은 dep 우회.

호출 패턴 (Airflow BashOperator bash_command):
    /opt/airflow/dbt-venv/bin/python /opt/airflow/dags/common/capture_metrics.py <table>

stdout 마지막 line = JSON dict. Airflow BashOperator 의 `do_xcom_push=True` 가
stdout 마지막 line 을 XCom 으로 push. post_compaction_report task 가 XCom pull
후 `json.loads` 로 dict 복원.

dbt-venv 안에 duckdb + pyiceberg 0.11.1 + `flink_jobs.lib` (PYTHONPATH=
/opt/airflow/repo-src) 모두 정공 import 가능.
"""

from __future__ import annotations

import json
import sys


def main(table: str) -> None:
    """Iceberg table 메트릭 측정 + JSON stdout."""
    from flink_jobs.lib.duckdb_iceberg import build_catalog

    catalog = build_catalog()
    iceberg_table = catalog.load_table(table)
    files = list(iceberg_table.scan().plan_files())
    n_files = len(files)
    total_bytes = sum(f.file.file_size_in_bytes for f in files)
    n_snapshots = len(list(iceberg_table.snapshots()))

    metrics = {
        "table": table,
        "files": n_files,
        "bytes": total_bytes,
        "snapshots": n_snapshots,
    }
    # Airflow BashOperator do_xcom_push=True 는 stdout 의 마지막 line 을 XCom push
    print(json.dumps(metrics))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <table>", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1])
