"""DAG 공용 callback — Discord webhook 발신 (env 빈 값이면 stdout fallback).

Airflow `on_failure_callback` 의 context dict 에서 task_instance / dag /
execution_date 추출 후 Discord 메시지 1건 발신. webhook 미설정 시 log.warning
로 fallback (placeholder behavior — Day 5 PR γ 시점 default).

Day 9 PR γ 추가 — `send_compaction_report`. iceberg_maintenance DAG 의
`post_compaction_report` task 가 PythonOperator 로 호출. XCom 의 before /
after 메트릭 pull → 압축률 계산 → Discord 발신 + stdout fallback.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any

log = logging.getLogger(__name__)


def send_discord_alert(context: dict[str, Any]) -> None:
    """Airflow on_failure_callback. DISCORD_WEBHOOK_URL 빈 값 시 stdout fallback."""
    webhook = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

    ti = context.get("task_instance")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "unknown"
    task_id = ti.task_id if ti else "unknown"
    execution_date = context.get("execution_date") or context.get("logical_date")
    log_url = ti.log_url if ti else None

    msg = f"DAG `{dag_id}` task `{task_id}` failed at {execution_date}.\nlog: {log_url}"

    if not webhook:
        log.warning("DISCORD_WEBHOOK_URL 미설정 — stdout fallback. msg: %s", msg)
        return

    payload = json.dumps({"content": msg}).encode("utf-8")
    req = urllib.request.Request(
        webhook,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=5)  # noqa: S310 (env-set webhook)
        log.info("Discord alert sent.")
    except Exception as exc:  # noqa: BLE001
        log.error("Discord alert send failed: %s", exc)


def send_compaction_report(**context: Any) -> None:
    """XCom 의 before / after 메트릭 pull → Discord 메시지 + 발신.

    PythonOperator callable 시그니처 (Airflow 가 `**context` 로 dict 전달).
    XCom 의 task_ids `snapshot_metrics_before` + `snapshot_metrics_after`
    의 return_value pull 후 압축률 계산.

    Day 9 PR γ commit 4 — XCom payload 가 BashOperator stdout 의 JSON
    string (이전 commit 2 의 PythonOperator return dict 와 다름). isinstance
    검사로 string 시 json.loads, dict 시 그대로 (backward compat 보장 —
    unit test 의 dict mock 도 PASS).

    webhook 미설정 / XCom None 둘 다 stdout fallback (no exception).
    """
    ti = context["task_instance"]
    before_raw = ti.xcom_pull(task_ids="snapshot_metrics_before")
    after_raw = ti.xcom_pull(task_ids="snapshot_metrics_after")

    if not before_raw or not after_raw:
        log.warning("XCom 미존재 — before=%s after=%s", before_raw, after_raw)
        return

    # BashOperator do_xcom_push=True 의 stdout 마지막 line = JSON string.
    # 이전 PythonOperator return dict 도 backward compat.
    try:
        before = json.loads(before_raw) if isinstance(before_raw, str) else before_raw
        after = json.loads(after_raw) if isinstance(after_raw, str) else after_raw
    except (json.JSONDecodeError, TypeError) as exc:
        log.error("XCom JSON parse fail: before=%s after=%s err=%s", before_raw, after_raw, exc)
        return

    file_reduction = (
        (before["files"] - after["files"]) / before["files"] * 100 if before["files"] else 0.0
    )
    byte_change = after["bytes"] - before["bytes"]

    table = before.get("table", "unknown")
    msg = (
        f"Iceberg compaction report — `{table}`\n"
        f"  files: {before['files']} → {after['files']} "
        f"({file_reduction:.1f}% 감소)\n"
        f"  bytes: {before['bytes']:,} → {after['bytes']:,} ({byte_change:+,})\n"
        f"  snapshots: {before['snapshots']} → {after['snapshots']}"
    )

    webhook = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook:
        log.warning("DISCORD_WEBHOOK_URL 미설정 — stdout fallback. msg: %s", msg)
        return

    payload = json.dumps({"content": msg}).encode("utf-8")
    req = urllib.request.Request(
        webhook,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=5)  # noqa: S310 (env-set webhook)
        log.info("Compaction report sent.")
    except Exception as exc:  # noqa: BLE001
        log.error("Compaction report send failed: %s", exc)
