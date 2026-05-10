"""Spark batch — silver 시간 partition 1개 재처리 (멱등 MERGE INTO).

Day 5 Task 5.7 시점은 **placeholder** — Airflow DAG 의 dynamic task mapping
구조 검증만. Day 9 Task 9.2 진입 시점에 본문 채움:

- `MERGE INTO silver.<table> USING bronze.<table> ON dedup_key`
- `WHEN MATCHED UPDATE SET ...` / `WHEN NOT MATCHED INSERT VALUES ...`
- 같은 partition 을 반복 실행해도 결과 동일 (멱등성).
- Day 9 의 `iceberg_maintenance` DAG 의 `rewrite_data_files` 와 dedup_key
  일관 (spec §10 — 1번 미해결 closure 의 동일 패턴).

CLI:
  python spark/jobs/backfill_silver_partition.py <partition> <table> <dry_run>
"""
from __future__ import annotations

import sys


def main(partition: str, table: str, dry_run: bool) -> None:
    print(
        f"[Day 5 placeholder] backfill_silver_partition "
        f"partition={partition} table={table} dry_run={dry_run}. "
        f"본격 본문 = Day 9 Task 9.2 진입 시점."
    )


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: backfill_silver_partition.py <partition> <table> <dry_run>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2], sys.argv[3].lower() == "true")
