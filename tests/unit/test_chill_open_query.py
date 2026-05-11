"""is_open_now pure 함수 단위 테스트 — 자정 넘김 / 24시간 / 정상 3종 모두 처리.

Day 8 Task 8.2 의 TDD 단계 — `src/api/lib_chill.py` 구현 전에 본 테스트가
6 FAIL 되어야 함 (`is_open_now` 미정의). 구현 후 6 PASS.

import 컨벤션 — Day 7 PR α 정착 (`from api.X`). pyproject `[tool.hatch.build.targets.wheel].packages`
에 `src/api` 등록되어 있어 `from api.lib_chill import is_open_now` 가 resolve.
"""

from __future__ import annotations

from api.lib_chill import is_open_now


def test_normal_hours_open():
    """정상 케이스 — 9~22 영업, 현재 14시 → 영업 중."""
    assert is_open_now(9, 22, 14) is True


def test_normal_hours_closed_before_open():
    """정상 케이스 — 9~22 영업, 현재 7시 → open 전."""
    assert is_open_now(9, 22, 7) is False


def test_normal_hours_closed_after_close():
    """정상 케이스 — 9~22 영업, 현재 23시 → close 후."""
    assert is_open_now(9, 22, 23) is False


def test_overnight_open_at_2am():
    """자정 넘김 — 18~4 영업, 현재 2시 → 영업 중 (`places_seed_sample.csv` 의 역삼 야간식당 시나리오)."""
    assert is_open_now(18, 4, 2) is True


def test_overnight_closed_at_10am():
    """자정 넘김 — 18~4 영업, 현재 10시 → close 후."""
    assert is_open_now(18, 4, 10) is False


def test_24h_always_open():
    """24시간 — 0~24, 어느 시각이든 항상 영업 (`places_seed_sample.csv` 의 강남 24시 김밥 시나리오)."""
    assert is_open_now(0, 24, 3) is True
    assert is_open_now(0, 24, 23) is True
