"""Spark submit helper — Day 9 와 공유.

Day 5 시점은 echo placeholder. Day 9 Task 9.2 진입 시점에 `spark-submit` 실
호출 + Iceberg connector + 멱등 MERGE INTO 본문 (`spark/jobs/
backfill_silver_partition.py` 와 함께 본격).
"""
from __future__ import annotations

import logging
import subprocess

log = logging.getLogger(__name__)


def submit_spark_backfill(partition: str, table: str, dry_run: bool) -> dict:
    """Day 5 placeholder — echo + 메타 dict 반환.

    Day 9 본격 — `spark-submit --master local[2] --packages org.apache.iceberg:
    iceberg-spark-runtime-3.5_2.12:1.7.1 spark/jobs/backfill_silver_partition.py
    {partition} {table} {dry_run}`.
    """
    cmd = (
        f"echo '[Day 5 placeholder] spark backfill "
        f"partition={partition} table={table} dry_run={dry_run}'"
    )
    log.info("submit_spark_backfill: %s", cmd)
    subprocess.run(cmd, shell=True, check=True)  # noqa: S602
    return {
        "partition": partition,
        "table": table,
        "dry_run": dry_run,
        "rows_processed": 0,
    }
