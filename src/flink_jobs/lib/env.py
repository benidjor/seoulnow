"""PyFlink streaming TableEnvironment 공통 설정.

`bronze_to_silver` / `silver_to_gold` (+ Day 5+ 추가 streaming job) 가
모두 같은 4 옵션 + classloader fix 를 사용. 옵션 누락 시 silent commit
fail 재발 가능 — 함수 안에서 묶어 drift 회피. Day 4 Task 1 fix 의
ClassLoader 옵션은 본 함수에서만 set, 다른 자리에서 set 금지.
"""
from __future__ import annotations

from pyflink.table import EnvironmentSettings, TableEnvironment

from flink_jobs.lib.classpath import flink_classpath


def build_streaming_env() -> TableEnvironment:
    """Streaming mode TableEnvironment 1개 생성 + 4 옵션 + classloader fix.

    옵션 (drift 시 streaming silent commit fail 재발):

    - `pipeline.jars` — connector / iceberg-flink-runtime / hadoop-client
      JAR 5종 classpath. 미지정 시 connector 미발견 fail.
    - `parallelism.default` = 1. local single-node 환경 (24GB RAM) 의
      기본 parallelism. 운영 deploy 시점에 재검토.
    - `execution.checkpointing.interval` = 30s. Iceberg sink commit 주기와
      정렬. 미지정 시 checkpoint 미트리거 → snapshot 0.
    - `table.dynamic-table-options.enabled` = true. SQL hint
      `/*+ OPTIONS(...) */` (silver 의 streaming/monitor-interval) 적용
      위해 명시적 enable. default false 라 미지정 시 hint 무시.
    - `classloader.parent-first-patterns.additional` =
      `com.codahale.metrics.;io.dropwizard.metrics.`. iceberg-flink-runtime
      jar 의 codahale/dropwizard metrics 클래스가 PyFlink JVM system
      classpath 와 ChildFirstClassLoader 에서 LinkageError 일으켜
      `IcebergStreamWriter.prepareSnapshotPreBarrier` 단계 매번 fail
      → silent commit fail. parent-first 강제 시 단일 loader 가 처리.
      Day 4 Task 1 silver fix 의 핵심 (archive
      `2026-05-08-day-4-task-1-silver-fix.md` §7 단계 7).

    restart-strategy 는 default (fixed-delay 무한 retry, streaming 표준).
    fail diagnose 가 필요할 때만 'none' override.
    """
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("pipeline.jars", flink_classpath())
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("execution.checkpointing.interval", "30 s")
    t_env.get_config().set("table.dynamic-table-options.enabled", "true")
    t_env.get_config().set(
        "classloader.parent-first-patterns.additional",
        "com.codahale.metrics.;io.dropwizard.metrics.",
    )
    return t_env
