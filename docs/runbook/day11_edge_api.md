# Day 11 구현 Runbook — gold compaction + FastAPI 실데이터 + HTTP receiver

Day 11 backend 운영 절차 정리. Task 11.1-A(gold compaction + FastAPI 안정화) + 11.2/11.3(HTTP receiver + `user.events.v1` 토픽) 의 수동 실행·검증 명령을 명문화한다. Edge API(events POST)는 Task 11.1-B 미완이라 §6 placeholder 로 둔다.

관련: PR #74(11.1-A) / #75(11.2-11.3), 정찰 `docs/portfolio/troubleshooting/2026-05-24-day-11-task-11.0-live-data-recon.md`.

## 1. gold compaction 수동 실행 + 검증

야간 DAG(§2) 외에 즉시 compaction 이 필요할 때의 절차. streaming 이 5분마다 micro-batch 로 gold 에 small-file 를 쌓으므로 누적 시 read latency 상승.

```bash
# 1. compaction 전 메트릭 (호스트 실행 — endpoint 는 localhost 고정, §5 참조)
MINIO_ENDPOINT=http://localhost:9000 LAKEKEEPER_URL=http://localhost:8181 \
  PYTHONPATH=src .venv/bin/python airflow/dags/common/capture_metrics.py \
  gold.fact_hotspot_congestion_5min silver.hotspot_congestion
#  → {"tables":[{"table":"gold...","files":9443,"snapshots":3421}, ...]}

# 2. gold rewrite_data_files (docker spark-submit, scp_default 네트워크 내부)
docker run --rm --network scp_default \
  -v "$PWD/infra/spark/conf:/opt/spark/conf:ro" \
  -v "$PWD/infra/spark/jobs:/workspace/jobs:ro" \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e AWS_REGION=us-east-1 \
  scp/spark:3.5.3-iceberg /opt/spark/bin/spark-submit /workspace/jobs/compaction_gold.py
#  → before: files=9443 ... after: files=3 ... file reduction: 100.0%

# 3. expire_snapshots (silver + gold, retain_last=5 — old data file 회수)
docker run --rm --network scp_default \
  -v "$PWD/infra/spark/conf:/opt/spark/conf:ro" \
  -v "$PWD/infra/spark/jobs:/workspace/jobs:ro" \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e AWS_REGION=us-east-1 \
  scp/spark:3.5.3-iceberg /opt/spark/bin/spark-submit /workspace/jobs/expire_snapshots.py
#  → gold...: snapshots 3421 -> 5 / silver...: snapshots 3429 -> 6
```

rewrite → expire 순서 의무. rewrite 가 새 큰 파일 + 새 snapshot 생성, expire 가 old snapshot + 그 snapshot 만 참조하던 small data file 회수.

## 2. nightly 자동 compaction (`iceberg_maintenance` DAG)

위 수동 절차는 `iceberg_maintenance` DAG 가 매일 03:00 KST 자동 실행한다(streaming peak 회피).

- task 7개: `snapshot_metrics_before` → `rewrite`(silver + gold 병렬 child) → `expire_snapshots` → `remove_orphan_files`(placeholder) → `snapshot_metrics_after` → `post_compaction_report`.
- 수동 트리거: `docker exec scp-airflow-scheduler airflow dags trigger iceberg_maintenance`.
- 구조 확인: `docker exec scp-airflow-scheduler airflow tasks list iceberg_maintenance | sort`.

## 3. FastAPI 호스트 실행 (`run_api.sh`)

chill-open / hotspots API 는 호스트(노트북)에서 uvicorn 으로 띄운다. Iceberg gold 데이터가 로컬 docker 스택(MinIO)에 있어 그 데이터를 읽어 Tunnel 로 노출하는 구조.

```bash
bash scripts/run_api.sh            # 127.0.0.1:8000, localhost endpoint 고정
# 검증
curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" http://127.0.0.1:8000/api/hotspots
```

- `run_api.sh` 가 `MINIO_ENDPOINT=http://localhost:9000` + `LAKEKEEPER_URL=http://localhost:8181` 을 env 로 주입(§5 Issue 1 회피). 환경변수 > `.env` 우선순위라 `.env` 의 `minio:9000` 을 override.
- compaction 후 기대 latency: cold 첫 요청 ~2s, warm ~35ms (compaction 전 11-29s 대비). serving cache 불필요.
- 동시성: 요청별 `duck_cursor()` + thread-local catalog 라 병렬 요청에 502 없음(정찰 Issue 3 해결).

## 4. HTTP receiver 실행 (`user.events.v1`)

익명 행동 이벤트를 Kafka 로 발행하는 receiver. 기본 스택에 안 뜨고 profile 로만 기동.

```bash
# 토픽 (멱등 — 이미 있으면 skip)
bash infra/kafka/create_topics.sh                 # user.events.v1 = partitions 6 / retention 30d

# receiver 기동 (profile=receiver, 기본 up 대상 아님)
docker compose --profile receiver up -d http-receiver

# smoke — 정상 200
curl -s -X POST http://127.0.0.1:8400/v1/events \
  -H "Authorization: Bearer $RECEIVER_TOKEN" -H "Content-Type: application/json" \
  -d '{"events":[{"event_id":"<uuid>","event_type":"map_view","anon_id":"<uuid>","ts":"2026-05-25T00:00:00Z"}]}'
#  → {"published":1}
# smoke — 잘못된 토큰 401
curl -s -o /dev/null -w "%{http_code}\n" -X POST http://127.0.0.1:8400/v1/events -H "Authorization: Bearer wrong" -d '{}'

# Kafka 수신 확인
docker exec scp-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic user.events.v1 --from-beginning --max-messages 1 --property print.headers=true
```

## 5. 환경 편차 / 운영 주의

호스트와 docker 내부의 hostname 해석 차이가 두 곳에서 문제를 일으킨다.

- **MinIO endpoint (호스트=localhost, compose 내부=minio)**: 호스트 FastAPI 의 pyiceberg(PyArrow S3, AWS C++ SDK)가 `minio` hostname 을 `/etc/hosts` alias 로 신뢰성 있게 해석 못 함 → 간헐 DNS 실패. `run_api.sh` 가 `localhost` 로 고정해 회피. docker/VM 내부 실행은 compose DNS 로 `minio` 정상.
- **Kafka dual-listener (INTERNAL `kafka:29092` vs EXTERNAL `localhost:9092`)**: docker 네트워크 안 컨테이너(receiver / kafka-connect)가 EXTERNAL listener 를 쓰면 self-loopback 으로 깨짐. receiver 는 `KAFKA_BOOTSTRAP_SERVERS=kafka:29092`(INTERNAL) 사용. 호스트에서 직접 producing 시에만 `localhost:9092`. plan 골격은 `kafka:9092` 였으나 본 compose 의 dual-listener 구성에 맞춰 INTERNAL 로 정정(Track C 발견).
- **streaming = 호스트 프로세스**: `flink_jobs.bronze_to_silver` + `silver_to_gold` + `producers.hotspot_producer` 가 docker 아닌 호스트에서 가동(`docker ps` 에 안 보임, `ps aux | grep flink_jobs` 로 확인). compaction 을 streaming 동시 쓰기 중 실행해도 Iceberg optimistic concurrency 로 안전.

## 6. Edge API (Task 11.1-B) — 미완

Day 11 의 마지막 조각. 본 runbook 에 후속 추가 예정.

- 브라우저 → Cloudflare Pages Edge API `POST /api/v1/events` → Cloudflare Tunnel → §4 receiver 로 forward.
- anon_id 쿠키 발급(1년, IP 영구저장 X) + event-validator(11.3 schema 정합).
- item 5: chill-open / hotspots API 용 안정 named Tunnel 셋업(§3 `run_api.sh` 기동 → cloudflared route) 후 Cloudflare Pages 의 `CHILL_API_BASE` env 재설정 → 배포된 지도가 실데이터 표시.
