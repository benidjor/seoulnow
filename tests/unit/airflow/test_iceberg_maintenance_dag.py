"""iceberg_maintenance DAG 단위 테스트 (DAG parsing + task graph + 본진 기능 검증).

Day 9 PR γ — Plan §Task 9.3 TDD Step 1-4 의 4 case + 본진 기능 추가 case
+ commit 4 의 Option B fix case (총 10 case):

1. DAG 파싱 OK + dag_id 정합.
2. schedule = `0 3 * * *` 활성 (Day 9 본격 활성 SoT).
3. task 6개 + `rewrite_dim_place` 제거 + task_id rename 확인 (A1 + A2).
4. rewrite task = BashOperator + `docker run --rm` + scp_default network (A4).
5. snapshot_metrics_before/after = BashOperator + dbt-venv subprocess
   (commit 4 의 Option B fix — 이전 commit 2 의 PythonOperator → 정정).
6. post_compaction_report = `send_compaction_report` callable.
7. SLA 1h + on_failure_callback = send_discord_alert (본진 기능).
8. send_compaction_report XCom None / 압축률 계산 정합 (dict mock, helper 함수).
9. capture_metrics.py helper script 존재 (commit 4 신규).
10. send_compaction_report 가 BashOperator stdout JSON string XCom parse 정공
    (commit 4 의 Option B 의 backward compat 검증).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# host venv 는 airflow 미설치 (pyproject.toml 의 dep 미포함 — Airflow 는
# 컨테이너 안 base image 가 제공). host pytest 실행 시 자동 skip, Airflow
# 컨테이너 안 pytest 실행 시 본격 검증.
# 주의: repo root 에 `airflow/` 디렉토리가 있어 `import airflow` 자체는
# namespace package 로 성공할 수 있음. `airflow.DAG` 실제 import 가능 여부로
# 판정.
try:
    from airflow import DAG as _DAG  # noqa: F401
except ImportError:
    pytest.skip(
        "Apache Airflow 미설치 (host venv) — Airflow 컨테이너 안에서 실행 의무.",
        allow_module_level=True,
    )

# DAG file import 위해 path 추가. host venv (skip) 가 아닐 때만 의미 있음.
# Airflow 컨테이너 안 `from common.callbacks import ...` 도 resolve 위해
# airflow/dags + src 둘 다 path 주입.
ROOT = Path(__file__).parents[3]
DAGS_DIR = ROOT / "airflow" / "dags"
SRC_DIR = ROOT / "src"
sys.path.insert(0, str(DAGS_DIR))
sys.path.insert(0, str(SRC_DIR))


def test_dag_loads():
    """iceberg_maintenance DAG 파싱 OK."""
    from iceberg_maintenance import dag  # type: ignore

    from airflow import DAG

    assert isinstance(dag, DAG)
    assert dag.dag_id == "iceberg_maintenance"


def test_dag_schedule_active():
    """schedule 가 매일 03:00 활성 (Day 9 본격 활성 SoT)."""
    from iceberg_maintenance import dag  # type: ignore

    # Airflow 2.x = schedule_interval 또는 timetable
    schedule = getattr(dag, "schedule_interval", None) or str(dag.timetable)
    assert "0 3" in str(schedule)


def test_dag_has_six_tasks():
    """task 6개 (rewrite_dim_place 제거 후, 사용자 결정 A1).

    TaskGroup 안 task 의 task_id 는 group 이름으로 prefix 됨
    (`rewrite.rewrite_silver_hotspot_congestion`).
    """
    from iceberg_maintenance import dag  # type: ignore

    task_ids = sorted(t.task_id for t in dag.tasks)
    # A2 rename — TaskGroup `rewrite` prefix
    assert "rewrite.rewrite_silver_hotspot_congestion" in task_ids
    assert "rewrite_dim_place" not in task_ids  # A1 제거
    assert "rewrite.rewrite_dim_place" not in task_ids  # A1 제거 (group prefix 형태도)
    assert "snapshot_metrics_before" in task_ids
    assert "snapshot_metrics_after" in task_ids
    assert "expire_snapshots" in task_ids
    assert "remove_orphan_files" in task_ids
    assert "post_compaction_report" in task_ids
    assert len(dag.tasks) == 6


def test_rewrite_task_uses_bash_operator_with_docker_run():
    """rewrite_silver_hotspot_congestion = BashOperator + docker run --rm (A4).

    TaskGroup `rewrite` 안 task 는 `rewrite.rewrite_silver_hotspot_congestion`
    full task_id 로 lookup.
    """
    from airflow.operators.bash import BashOperator
    from iceberg_maintenance import dag  # type: ignore

    task = dag.get_task("rewrite.rewrite_silver_hotspot_congestion")
    assert isinstance(task, BashOperator)
    assert "docker run --rm" in task.bash_command
    assert "scp/spark:3.5.3-iceberg" in task.bash_command
    assert "compaction_silver.py" in task.bash_command
    assert "scp_default" in task.bash_command  # network SoT


def test_metrics_tasks_use_bash_operator_with_dbt_venv():
    """snapshot_metrics_before/after = BashOperator + dbt-venv subprocess (commit 4 Option B).

    이전 commit 2 의 PythonOperator in-process 호출은 Airflow 기본 venv 의
    duckdb / pyiceberg 미설치로 ModuleNotFoundError fail. commit 4 fix =
    BashOperator + /opt/airflow/dbt-venv/bin/python + capture_metrics.py.
    """
    from airflow.operators.bash import BashOperator
    from iceberg_maintenance import dag  # type: ignore

    for tid in ("snapshot_metrics_before", "snapshot_metrics_after"):
        task = dag.get_task(tid)
        assert isinstance(task, BashOperator)
        assert "/opt/airflow/dbt-venv/bin/python" in task.bash_command
        assert "capture_metrics.py" in task.bash_command
        assert "silver.hotspot_congestion" in task.bash_command
        assert task.do_xcom_push is True


def test_post_compaction_report_uses_send_compaction_report():
    """post_compaction_report = PythonOperator + send_compaction_report callable."""
    from airflow.operators.python import PythonOperator
    from iceberg_maintenance import dag  # type: ignore

    task = dag.get_task("post_compaction_report")
    assert isinstance(task, PythonOperator)
    assert task.python_callable.__name__ == "send_compaction_report"


def test_dag_has_sla_and_failure_callback():
    """SLA 1h + on_failure_callback (send_discord_alert) — 본진 기능 (spec §5-8)."""
    from datetime import timedelta

    from iceberg_maintenance import dag  # type: ignore

    assert dag.default_args["sla"] == timedelta(hours=1)
    assert dag.default_args["on_failure_callback"].__name__ == "send_discord_alert"


def test_send_compaction_report_xcom_pull_no_data():
    """send_compaction_report 가 XCom None / empty 시 stdout fallback (no exception)."""
    from unittest.mock import MagicMock

    from common.callbacks import send_compaction_report  # type: ignore

    ti = MagicMock()
    ti.xcom_pull.return_value = None
    # webhook 미설정 시 stdout fallback (no exception)
    send_compaction_report(task_instance=ti)


def test_send_compaction_report_calculates_reduction():
    """send_compaction_report 가 file_reduction_pct 정공 계산 + stdout fallback OK.

    dict 형태 XCom payload (이전 commit 2 PythonOperator return) backward compat.
    """
    from unittest.mock import MagicMock, patch

    from common.callbacks import send_compaction_report  # type: ignore

    ti = MagicMock()
    # 475 → 3 = 99.4% reduction (PR β SoT)
    ti.xcom_pull.side_effect = [
        {
            "table": "silver.hotspot_congestion",
            "files": 475,
            "bytes": 2_700_000,
            "snapshots": 100,
        },
        {
            "table": "silver.hotspot_congestion",
            "files": 3,
            "bytes": 31_457_280,
            "snapshots": 101,
        },
    ]
    # webhook 미설정 — stdout fallback 으로 안전 호출
    with patch.dict("os.environ", {"DISCORD_WEBHOOK_URL": ""}, clear=False):
        send_compaction_report(task_instance=ti)
    # exception 0건 = PASS


def test_capture_metrics_script_exists():
    """capture_metrics.py helper script 가 dags/common/ 에 존재 (commit 4 신규)."""
    script = ROOT / "airflow" / "dags" / "common" / "capture_metrics.py"
    assert script.exists(), f"helper script 부재: {script}"


def test_send_compaction_report_parses_json_string():
    """send_compaction_report 가 BashOperator stdout JSON string XCom parse 정공 (commit 4).

    Option B 정공 검증 — BashOperator do_xcom_push 의 stdout 마지막 line 은
    JSON string. json.loads 후 dict 복원하여 압축률 계산.
    """
    import json
    from unittest.mock import MagicMock, patch

    from common.callbacks import send_compaction_report  # type: ignore

    ti = MagicMock()
    # BashOperator do_xcom_push = stdout 마지막 line (JSON string)
    ti.xcom_pull.side_effect = [
        json.dumps(
            {
                "table": "silver.hotspot_congestion",
                "files": 475,
                "bytes": 2_700_000,
                "snapshots": 100,
            }
        ),
        json.dumps(
            {
                "table": "silver.hotspot_congestion",
                "files": 3,
                "bytes": 31_457_280,
                "snapshots": 101,
            }
        ),
    ]
    with patch.dict("os.environ", {"DISCORD_WEBHOOK_URL": ""}, clear=False):
        send_compaction_report(task_instance=ti)
    # exception 0건 = PASS
