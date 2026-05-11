"""Streaming job lifecycle helpers — long-running 운영 + smoke 검증 양립.

`bronze_to_silver` / `silver_to_gold` / `cdc_to_dim_place` 가 모두 같은 패턴
(`time.sleep(SMOKE_RUN_SECONDS)`) 을 가지고 있어 PR δ §1 의 silent timeout exit
이 3 job 모두에 영향. 본 lib 가 단일 출처로 lifecycle 을 처리.

환경변수 `FLINK_SMOKE_RUN_SECONDS`:
  - 미설정 또는 0 (default) → long-running 모드. SIGTERM/SIGINT 까지 대기,
    1h heartbeat 로 alive 가시성 확보 (PR δ §6-1 silent exit fingerprint 회피).
  - >0 → smoke mode. N초 후 자연 종료, 단 SIGTERM 도 graceful 처리.

기존 동작 (default 600 = 10분 자동 종료) 은 PR δ §1 한계의 원인이라 default 변경.
smoke 검증을 원하면 명시적으로 `FLINK_SMOKE_RUN_SECONDS=600` 환경변수 export 의무.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
from types import FrameType

log = logging.getLogger(__name__)


def wait_for_shutdown() -> None:
    """SMOKE_RUN_SECONDS 환경변수에 따라 streaming main 을 대기시킨다.

    `t_env.execute_sql(insert_sql)` 같은 async submit 직후 호출. mini-cluster
    LocalEnvironment 는 detached mode 미지원이라 main 종료 = background job 종료.
    """
    smoke_seconds = int(os.environ.get("FLINK_SMOKE_RUN_SECONDS", "0"))
    shutdown = threading.Event()

    def _handle_signal(_signum: int, _frame: FrameType | None) -> None:
        log.info("Received SIGTERM/SIGINT, shutting down.")
        shutdown.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    if smoke_seconds > 0:
        log.info("Streaming 가동 중 (smoke mode, %ds). SIGTERM 대기.", smoke_seconds)
        if shutdown.wait(timeout=smoke_seconds):
            log.info("SIGTERM 수신, 종료.")
        else:
            log.info("Smoke run timeout (%ds), 종료.", smoke_seconds)
        return

    log.info("Streaming 가동 중 (long-running mode). SIGTERM 대기, 1h heartbeat.")
    while not shutdown.is_set():
        if shutdown.wait(timeout=3600):
            break
        log.info("alive (1h heartbeat).")
    log.info("SIGTERM 수신, 종료.")
