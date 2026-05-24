"""GET /api/chill-open → 한가 + 영업 중 가게 리스트.

Day 8 Task 8.2.

deviation 8.2-A (사전 채택) — plan 본문 (line 1854~1888) 의 inline `CREATE OR REPLACE SECRET`
+ `iceberg_scan('{base}/gold/...')` 를 lib reuse 패턴으로 변경.

- gold parquet path 는 `flink_jobs.lib.duckdb_iceberg.table_paths()` 우회 — Day 4
  archive `2026-05-09-day-4-tasks-4_1-4_3.md` 학습. Lakekeeper REST UUID-prefix
  path 를 DuckDB `iceberg_scan` 이 resolve 못함.
- DuckDB SECRET DDL 은 `api.deps.duck_connection()` 이 lib `configure_duckdb`
  위임으로 한 곳에서 처리. route 는 요청별 `duck_cursor()` (= 같은 connection 의
  `.cursor()`) 를 써서 SECRET 을 공유하면서 threadpool 동시 요청에 안전.
  SQL injection 표면 lib `_quote_literal` 한 곳.
- `bronze.places_static` 은 정적 parquet 라 lib catalog 미경유 — DuckDB `read_parquet()`
  로 직접 read. settings 에서 warehouse bucket 만 가져옴.

출처: Day 7 PR γ archive §10-4 (lib reuse 5번째 consumer) → 본 route 가 7번째 consumer.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter

from api.deps import duck_cursor, gold_table_paths
from api.lib_chill import is_open_now
from platform_common import get_settings

router = APIRouter(prefix="/api", tags=["chill-open"])


def _row_to_dict(cols: list[str], row: tuple) -> dict[str, Any]:
    """datetime 류는 ISO-8601 직렬화 (`hotspots._row_to_dict` 와 동일 컨벤션)."""
    out: dict[str, Any] = {}
    for col, val in zip(cols, row, strict=True):
        out[col] = val.isoformat() if hasattr(val, "isoformat") else val
    return out


@router.get("/chill-open")
def chill_open() -> dict[str, Any]:
    """한가 (district avg score <= 2) + active 가게 중 현재 영업중 list.

    응답 schema:
      {"items": [{"biz_reg_no", "name", "category", "district", "latitude",
                  "longitude", "open_hour", "close_hour", "avg_congest_score",
                  "is_open_now"}, ...],
       "count": N,
       "current_hour": H}
    """
    paths = gold_table_paths()
    if not paths:
        # gold streaming snapshot 0 → 비어있는 응답. current_hour 는 그대로 회신
        # (debug 편의 — 시각만 확인하고 싶을 때 fast-path).
        return {"items": [], "count": 0, "current_hour": datetime.now().hour}

    s = get_settings()
    places_static_path = (
        f"s3://{s.iceberg_warehouse_bucket}/warehouse/bronze/places_static_v1/data.parquet"
    )

    con = duck_cursor()
    rows = con.execute(
        """
        WITH district_score AS (
            SELECT district, avg_congest_score
            FROM (
                SELECT
                    district,
                    avg_congest_score,
                    row_number() OVER (
                        PARTITION BY district ORDER BY window_start DESC
                    ) AS rn
                FROM read_parquet(?, hive_partitioning = true)
            )
            WHERE rn = 1
        ),
        places AS (
            SELECT *
            FROM read_parquet(?)
            WHERE status = 'active'
        )
        SELECT
            p.biz_reg_no,
            p.name,
            p.category,
            p.district,
            p.latitude,
            p.longitude,
            p.open_hour,
            p.close_hour,
            d.avg_congest_score
        FROM places p
        JOIN district_score d USING (district)
        WHERE d.avg_congest_score <= 2
        ORDER BY d.avg_congest_score ASC, p.name
        """,
        [paths, places_static_path],
    ).fetchall()
    cols = [
        "biz_reg_no",
        "name",
        "category",
        "district",
        "latitude",
        "longitude",
        "open_hour",
        "close_hour",
        "avg_congest_score",
    ]

    current_hour = datetime.now().hour
    items: list[dict[str, Any]] = []
    for row in rows:
        record = _row_to_dict(cols, row)
        record["is_open_now"] = is_open_now(record["open_hour"], record["close_hour"], current_hour)
        if record["is_open_now"]:
            items.append(record)
    return {"items": items, "count": len(items), "current_hour": current_hour}
