"""Day 11 Task 11.1-A: gold.fact_hotspot_congestion_5min small file 압축.

compaction_silver.py 미러. before/after metric 출력. 원래 plan(phase-1a-week-2.md:2323)
의 rewrite_fact_hotspot_congestion_5min 의도를 gold 에 한해 복원.

deviation reuse (compaction_silver.py SoT 동일):
- 9.1-A: Lakekeeper warehouse = `seoul`
- 9.1-B: iceberg-aws-bundle
- 9.1-C: extraClassPath 절대 경로
- 9.2-A: namespace.table 2-part identifier (`gold.fact_hotspot_congestion_5min`)
"""

from __future__ import annotations

import time

from pyspark.sql import SparkSession

TABLE = "gold.fact_hotspot_congestion_5min"


def session() -> SparkSession:
    return SparkSession.builder.appName("scp.day11.compaction_gold").getOrCreate()


def file_count(spark: SparkSession) -> tuple[int, float]:
    df = spark.sql(f"""
        SELECT count(*) AS n, avg(file_size_in_bytes)/1024/1024.0 AS avg_mb
        FROM ice.{TABLE}.files
    """)
    row = df.collect()[0]
    return int(row["n"]), float(row["avg_mb"] or 0.0)


def query_time(spark: SparkSession) -> float:
    start = time.time()
    spark.sql(f"""
        SELECT district, count(*) c, avg(avg_congest_score) s
        FROM ice.{TABLE}
        GROUP BY district
    """).collect()
    return time.time() - start


def main() -> None:
    spark = session()

    n_before, mb_before = file_count(spark)
    t_before = query_time(spark)
    print(
        f"before: files={n_before} avg_size_mb={mb_before:.3f} "
        f"group_by_query_seconds={t_before:.2f}"
    )

    spark.sql(f"""
        CALL ice.system.rewrite_data_files(
            table => '{TABLE}',
            options => map('target-file-size-bytes', '134217728')
        )
    """)

    n_after, mb_after = file_count(spark)
    t_after = query_time(spark)
    print(
        f"after : files={n_after} avg_size_mb={mb_after:.3f} group_by_query_seconds={t_after:.2f}"
    )

    if n_before > 0:
        reduction_pct = 100.0 * (n_before - n_after) / n_before
        print(f"file reduction: {reduction_pct:.1f}%")


if __name__ == "__main__":
    main()
