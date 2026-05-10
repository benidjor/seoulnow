"""DAG 공용 callback — Discord webhook 발신 (env 빈 값이면 stdout fallback).

Airflow `on_failure_callback` 의 context dict 에서 task_instance / dag /
execution_date 추출 후 Discord 메시지 1건 발신. webhook 미설정 시 log.warning
로 fallback (placeholder behavior — Day 5 PR γ 시점 default).
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

    msg = (
        f"DAG `{dag_id}` task `{task_id}` failed at {execution_date}.\n"
        f"log: {log_url}"
    )

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
