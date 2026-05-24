"""api.deps thread-safety 단위 — thread-local catalog + cursor 분리."""
from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).parents[3]
sys.path.insert(0, str(ROOT / "src"))

pytest.importorskip("duckdb")
pytest.importorskip("pyiceberg")


def test_catalog_is_thread_local():
    """서로 다른 스레드는 서로 다른 catalog 인스턴스를 받고, 같은 스레드는 재사용한다.

    Barrier 로 N개 워커가 동시에 catalog() 에 진입하도록 강제 — trivial 작업에서
    ThreadPoolExecutor 가 단일 스레드를 재사용해 인스턴스가 1개로 줄어드는 비결정성
    제거. 진짜 process-singleton(lru_cache)이면 N개 스레드가 같은 인스턴스를 받아
    distinct 수가 1이 되므로, 본 테스트가 thread-local 과 singleton 을 구별한다.
    """
    from api import deps

    n = 4
    barrier = threading.Barrier(n)
    created: list[object] = []
    lock = threading.Lock()

    def _fake_build():
        c = MagicMock()
        with lock:
            created.append(c)
        return c

    def _worker(_: int) -> tuple[int, int]:
        barrier.wait()  # N개 스레드가 동시에 살아있도록 강제
        first = deps.catalog()
        second = deps.catalog()  # 같은 스레드 재호출 → 동일 인스턴스여야
        return id(first), id(second)

    with patch.object(deps, "build_catalog", side_effect=_fake_build):
        # thread-local 초기화 (이전 테스트 잔여 제거)
        if hasattr(deps._tls, "catalog"):
            del deps._tls.catalog

        with ThreadPoolExecutor(max_workers=n) as ex:
            results = list(ex.map(_worker, range(n)))

    # N개 스레드 → N개 distinct 인스턴스 (singleton 이면 1)
    assert len({first for first, _ in results}) == n
    # 같은 스레드 안 재호출은 동일 인스턴스 (thread-local 재사용)
    assert all(first == second for first, second in results)
    # build 는 스레드당 정확히 1회
    assert len(created) == n


def test_duck_cursor_calls_cursor():
    """duck_cursor() 는 싱글톤 연결의 .cursor() 를 반환."""
    from api import deps

    fake_conn = MagicMock()
    with patch.object(deps, "duck_connection", return_value=fake_conn):
        deps.duck_cursor()
    fake_conn.cursor.assert_called_once()
