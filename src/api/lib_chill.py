"""is_open_now pure 판정 — 자정 넘김 / 24시간 / 정상 케이스 모두 처리.

Day 8 Task 8.2. DuckDB / dbt 의 `extract(hour from now())` 계산은 timezone /
clock skew 영향을 받으므로 API 응답 시점에 Python `datetime.now().hour` 로
재계산하는 책임 분리 — 본 함수가 그 단위.

판정 규칙:
- `open_hour` 또는 `close_hour` 가 None → False (방어).
- `close_hour == 24 and open_hour == 0` → 항상 True (24시간 영업, e.g. 강남 24시 김밥).
- `close_hour > open_hour` (정상) → `open <= current < close`.
- `close_hour <= open_hour` (자정 넘김, e.g. 18~4) → `current >= open or current < close`.

테스트: `tests/unit/test_chill_open_query.py` 6 case (TDD red → green 전환).
"""

from __future__ import annotations


def is_open_now(open_hour: int | None, close_hour: int | None, current_hour: int) -> bool:
    """주어진 영업시각 범위와 현재 시각(hour) 으로 영업 중 여부 판정."""
    if open_hour is None or close_hour is None:
        return False
    if close_hour == 24 and open_hour == 0:
        return True
    if close_hour > open_hour:
        return open_hour <= current_hour < close_hour
    # 자정 넘김 (e.g. 18→4): current 가 open 이후이거나 close 이전.
    return current_hour >= open_hour or current_hour < close_hour
