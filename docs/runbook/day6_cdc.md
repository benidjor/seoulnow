# Day 6 CDC Runbook

> 작성: 2026-05-11
> 영역: Day 6 Task 6.1~6.4 (Kafka Connect + Debezium Postgres connector + PyFlink CDC 컨슈머 + dbt mart)
> 관련 PR: #36 (Task 6.1~6.2), #37 (image tag 정정), 본 PR β (Task 6.3~6.4)

## 정상 경로

```bash
# 1. docker compose 5종 가동 (kafka / kafka-connect / lakekeeper / minio / postgres)
docker compose up -d
# kafka-connect healthy 까지 30~60초 대기
docker compose ps

# 2. places seed (재실행 가능 — biz_reg_no UNIQUE 라 멱등)
docker compose exec -T postgres psql -U scp -d scp < infra/postgres/seed_places.sql

# 3. Debezium connector 등록 (이미 RUNNING 이면 skip)
./infra/debezium/register.sh

# 4. PyFlink CDC 컨슈머 가동 (background)
uv run --extra flink python -m flink_jobs.cdc_to_dim_place &
```

snapshot 5건 (biz_reg_no `1208612345`~`1208612349`) 이 30~60초 안에 `place.master.cdc.v1` 토픽 → `silver.dim_place` 까지 도달.

## CDC 검증 한 줄

```bash
docker compose exec -T postgres psql -U scp -d scp \
  -c "UPDATE places SET name = name || ' *' WHERE biz_reg_no='1208612345';"
```

이후 30~60초 안에 `silver.dim_place` 에 `cdc_op='u'` 한 행 추가. Lakekeeper REST 가 vending 하는 UUID-prefix path 회피용 read 패턴은 본 PR description 검증 절차의 5번째 명령 참조.

## 토픽 레벨 message 직접 검증 (디버깅)

Apache Kafka 4.0 컨테이너에서 console-consumer 로 raw message 확인:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic place.master.cdc.v1 \
  --from-beginning \
  --max-messages 6
```

Expected: 6 메시지 (snapshot 5 + UPDATE 1 이상). 각 message 는 `{schema, payload}` wrapping 구조이며, `payload.op` 은 snapshot 시 `r`, INSERT/UPDATE/DELETE 시 각각 `c`/`u`/`d`.

## fallback 트리거 (spec §9-1 Day 6)

Debezium 셋업이 4시간 초과로 막히면 다음 우회로 전환 — Phase 1A 의 SCD2 골격 산출물은 그대로 유지.

1. `places` 옆에 `places_outbox(place_id, op, payload_json, created_at)` 신규 테이블
2. application trigger 가 INSERT/UPDATE/DELETE 마다 outbox 에 한 행 append
3. 폴링 producer 가 `created_at > last_seen` 만 읽어 `place.master.cdc.v1` 으로 발행
4. PyFlink CDC 컨슈머 + dbt mart 는 변경 없음 (envelope 만 동일하게 맞춰서 발행)
5. Portfolio 에 "Debezium 도입 시도 + 폴링 fallback 트레이드오프" 솔직히 기술

## 자주 발생하는 문제

| 증상 | 원인 / 조치 |
|---|---|
| connector status `FAILED` + `replication slot already exists` | Postgres replication slot 잔존. `docker compose exec postgres psql -U scp -d scp -c "SELECT pg_drop_replication_slot('scp_places_slot');"` 후 connector 재등록. |
| Flink job 이 모든 row 를 silently drop (silver.dim_place 0행) | source DDL 의 `payload.op` unwrap 누락. envelope 이 `{schema, payload}` wrapping 구조라 root 의 `op` 은 항상 NULL. |
| 토픽이 `scp.public.places` 로 만들어짐 (RegexRouter 미적용) | `connector-places.json` 의 `transforms.rename.*` 설정 재확인 후 connector 삭제 + 재등록. |
| `VALUE_CONVERTER_SCHEMAS_ENABLE=false` 인데도 schema field 가 토픽 message 에 포함됨 | Debezium 2.7.x 의 알려진 동작. 본 PR 의 PyFlink source DDL 은 `{schema, payload}` wrapping 을 전제로 unwrap. |
| Flink 가동 직후 `IcebergStreamWriter.prepareSnapshotPreBarrier` LinkageError | `flink_jobs.lib.env.build_streaming_env()` 의 `classloader.parent-first-patterns.additional` 옵션 확인 (Day 4 Task 1 silver fix archive 참조). |

## 메모리 mitigation

CDC 컨슈머 (Flink) + 기존 hotspot/subway streaming + Airflow scheduler 동시 가동 시 24GB RAM 의 80% (19.2GB) 임계 가까워질 수 있음. Day 9 Spark 일시 기동 시점에는 다음 회수 절차:

```bash
# Day 9 Spark MERGE 직전
docker compose stop airflow-scheduler
# 또는 CDC 컨슈머 일시 중단
kill <cdc_to_dim_place pid>
```

## 관련 문서

- spec §4-2 (`dim_place` SCD2 정의), §6-1 Day 6, §9-1 Day 6 fallback
- Phase 1A Week 2 plan §Day 6 Task 6.1~6.4
- Day 4 Task 1 silver fix archive (classloader fix 의 단일 출처)
- Day 5 dbt runbook (`day5_dbt.md`) — dbt 명령 / profiles.yml 정책 공유
