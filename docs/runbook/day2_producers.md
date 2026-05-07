# Day 2 Producers Runbook

서울 OpenAPI 두 종 (도시데이터 핫스팟 + 지하철 도착정보) 을 Kafka 토픽으로 발행하는 host-측 Python producer 운영 매뉴얼.

> **이전 day**: [`day1_infra.md`](./day1_infra.md) (Day 1 인프라)
>
> **이후 day**: [`day3_pyflink.md`](./day3_pyflink.md) → [`day4_silver_fix.md`](./day4_silver_fix.md)

> **2026-05-05 갱신** — subway producer 의 endpoint pivot (`realtimeCongestion` → `realtimeStationArrival`) 반영. 무료 실시간 객차 혼잡도 API 부재 사유는 [`2026-05-05-day-3-pre-entry.md` §2](../portfolio/troubleshooting/2026-05-05-day-3-pre-entry.md#2-subway-url-placeholder--실시간-객차-혼잡도-api-부재) 참조. 지역의 인구 혼잡도는 hotspot producer (도시데이터 API) 가 담당.

## 사전 조건

- Day 1 인프라가 healthy 상태 ([`day1_infra.md`](./day1_infra.md))
- Kafka 토픽 4개 (`seoul.hotspot.congestion.v1`, `seoul.transit.subway.v1`, `place.master.cdc.v1`, `user.events.v1`) 생성됨
- `.env` 에 `SEOUL_OPENAPI_KEY` / `SEOUL_SUBWAY_API_KEY` 가 `replace-me` 가 아닌 실 키로 설정됨

## 평소 기동

### Python 환경

```bash
uv sync --extra dev                   # .venv 동기화 (의존성 91 / 설치 61)
```

### Producer 두 종 백그라운드 실행

```bash
uv run python -m producers.hotspot_producer &     # 5분 polling, 핫스팟 3곳
uv run python -m producers.subway_producer &      # 60초 polling, 2호선 / 9호선
```

`__main__` 의 `DEFAULT_AREAS` / `DEFAULT_LINES` 가 기본값. 다른 지역 / 노선 추가는 코드 직접 수정 또는 다음과 같이 작성:

```python
from producers.hotspot_producer import run as run_hotspot
run_hotspot({"POI004": "이태원", "POI005": "잠실"})
```

## 정지

```bash
# graceful shutdown — SIGTERM (또는 Ctrl+C)
kill -TERM <pid>
# 시그널 받으면 try/finally 의 producer.flush(timeout=10) 가 in-flight 메시지 보장
```

`docker compose down` 자체는 producer 와 무관 (host-측 프로세스). Kafka broker 만 정지하려면 broker 재기동 후 producer 도 재시작.

## 검증 — 토픽 메시지 확인

`api_response_ts` 헤더 포함 메시지 3개 확인:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic seoul.hotspot.congestion.v1 \
  --from-beginning --max-messages 3 --property print.headers=true \
  --timeout-ms 15000
```

기대 출력:

```
api_response_ts:2026-05-02T14:25:00,schema_version:v1,source:seoul.openapi.citydata
{"area_code": "POI001", "area_name": "강남역", "congest_level": "붐빔", ...}
```

지하철 토픽도 동일 패턴 (토픽 이름만 `seoul.transit.subway.v1`).

## 로그 확인

structlog JSON 출력 — 주요 이벤트:

| 이벤트 | 의미 | 정상 / 비정상 |
|------|------|------|
| `produced` | 핫스팟 1곳 메시지 발행 (count=1) | 정상 |
| `produced_batch` | 지하철 1 cycle 도착정보 배치 발행 (count=N, station=(all)) | 정상 (count > 0) |
| `parse_returned_none` | `parse_hotspot_payload` 결측 응답 | 비정상 — API 응답 형식 변경 의심 |
| `parse_returned_empty` | `parse_subway_payload` 가 빈 list 반환 | 비정상 — `errorMessage` 코드 / `recptnDt` 파싱 실패 / 빈 `realtimeArrivalList` |
| `stts_unexpected_first_element` | hotspot 의 `_unwrap_first` 가 비-dict 첫 원소 만남 (type 필드 포함) | 비정상 — API 응답 형태 또 변경. 즉시 schema 재조사 |
| `fetch_failed_http` (`status=4xx/5xx`) | API 응답 오류 | 401/403 → 키 검증 / 5xx → API 일시 장애 (tenacity retry 3회) |
| `fetch_failed` (`error=...`) | 네트워크 / DNS / 타임아웃 | tenacity retry 3회 후 fail. 다음 cycle 에서 재시도 |
| `shutdown signal received` | SIGINT / SIGTERM | 정상 종료 진행 중 |
| `skip_bad_response_time` (debug) | `responseTime` ValueError | 디폴트 INFO level 에선 안 보임. `--log-level=DEBUG` 활성화 시 노출 |

## 자주 발생하는 문제

| 증상 | 원인 / 조치 | 상세 |
|------|------|------|
| `SystemExit: SEOUL_OPENAPI_KEY not set` | `.env` 의 `SEOUL_OPENAPI_KEY` 가 `replace-me` 또는 빈 값 | API 키 발급 후 `.env` 갱신 |
| `SystemExit: SEOUL_SUBWAY_API_KEY not set` | 동일 (`SEOUL_SUBWAY_API_KEY`) | — |
| Kafka broker connection refused | Day 1 인프라가 안 떠있음 | `docker compose up -d` + `./scripts/healthcheck.sh` |
| 토픽이 없음 (`UNKNOWN_TOPIC_OR_PARTITION`) | Day 1 의 `scripts/create_topics.sh` 가 안 돌았음 | `./scripts/create_topics.sh` 또는 healthcheck 재실행 |
| `produced_batch (count=0)` 만 반복 | 빈 응답 / 파싱 실패 / API 오류 (subway 만 해당) | 다음 cycle 의 `parse_returned_empty` warning 확인 → 원인 식별 | [Issue 5](../portfolio/troubleshooting/2026-05-02-day-2-producers-troubleshooting.md#5-subway-빈-응답-가시화-운영-디버깅) |
| `fetch_failed_http (status=401)` 반복 | API 키 검증 실패 또는 키 quota 초과 | 발급 기관에 키 상태 확인 |
| `fetch_failed (error=RetryError)` 반복 | URL placeholder / endpoint 변경 / DNS 실패 | URL 점검 — `swopenapi.seoul.go.kr` 도메인 + `realtimeStationArrival` 서비스명 확인. [archive §2](../portfolio/troubleshooting/2026-05-05-day-3-pre-entry.md#2-subway-url-placeholder--실시간-객차-혼잡도-api-부재) |
| `AttributeError: 'list' object has no attribute 'get'` (hotspot) | 실 API 의 `*_STTS` 가 list, fixture 와 schema 불일치 | `_unwrap_first` helper 확인. fixture 갱신 후 단위 테스트 추가. [archive §1](../portfolio/troubleshooting/2026-05-05-day-3-pre-entry.md#1-hotspot-producer-의-실-api-list-wrapped-_stts-호환) |
| console-consumer 가 timeout (broker offset 은 정상) | KRaft single-node 의 `__consumer_offsets` 토픽이 RF=3 default 로 stuck | docker-compose.yml 에 `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` 외 2종 명시 후 재기동. [archive §3](../portfolio/troubleshooting/2026-05-05-day-3-pre-entry.md#3-kafka-kraft-single-node-의-internal-토픽-생성-stuck) |
| Kafka 재기동 시 `DUPLICATE_BROKER_REGISTRATION` | KRaft 의 broker session lease 가 만료 안 됨 (controller 와 broker 가 같은 프로세스) | `docker compose down kafka && docker compose up -d kafka` (컨테이너 재생성, volume 보존) |
| producer 종료 시 메시지 손실 의심 | finally 의 `producer.flush()` 가 안 호출되는 비정상 경로 | `confluent_kafka.Producer` 는 `close()` 없음 — flush 가 commit 보장. 비정상 경로는 [Issue 4](../portfolio/troubleshooting/2026-05-02-day-2-producers-troubleshooting.md#4-producer-flush-누락-데이터-안정성--streaming) 참조 |
| `repr(settings)` 시 `minio_password` 평문 노출 의심 | 옛 `str` 타입 사용 잔존 | 본 PR 에서 `SecretStr` 도입 — `'**********'` 마스킹 확인. [Issue 2](../portfolio/troubleshooting/2026-05-02-day-2-producers-troubleshooting.md#2-minio_password-평문-노출-보안--sot-일관성) |
| URL 경로에 평문 키가 로그에 박힘 의심 | `str(e)` 에 URL 포함 가능 | 본 PR 에서 except 분기 분리 — `status_code` / `type(e).__name__` 만. [Issue 3](../portfolio/troubleshooting/2026-05-02-day-2-producers-troubleshooting.md#3-url-경로-api-키-노출-위험-보안--구조적) |

## 통합 검증 (Day 3 진입 전 1회)

API 키 발급 후 다음 시퀀스로 실 토픽 발행 검증:

```bash
# 1. .env 갱신 확인
grep '^SEOUL_OPENAPI_KEY=\|^SEOUL_SUBWAY_API_KEY=' .env  # replace-me 가 아닐 것

# 2. producer 백그라운드 실행
uv run python -m producers.hotspot_producer &
HOTSPOT_PID=$!
sleep 30

# 3. 토픽 메시지 확인 (헤더 포함)
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic seoul.hotspot.congestion.v1 \
  --from-beginning --max-messages 3 --property print.headers=true \
  --timeout-ms 15000

# 4. graceful shutdown
kill -TERM $HOTSPOT_PID
```

기대:

- 메시지 본문에 `area_code`, `area_name`, `congest_level` 필드
- 헤더에 `api_response_ts`, `schema_version=v1`, `source=seoul.openapi.citydata`
- `producer.flush()` 가 finally 에서 마지막 호출되며 in-flight 메시지 broker 도달

지하철도 동일 절차 (토픽 / 모듈 이름만 변경).

### 대안 — broker offset 으로 직접 검증

console-consumer 가 KRaft single-node 의 internal 토픽 stuck 으로 timeout 시 broker 측에서 직접 메시지 commit 확인:

```bash
docker exec scp-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 \
  --topic seoul.hotspot.congestion.v1
# 출력: seoul.hotspot.congestion.v1:0:N (각 partition end offset)
# 합계 > 0 이면 broker 에 메시지 commit 됨

docker exec scp-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
# __consumer_offsets 가 list 에 있으면 consumer group 정상 동작 가능
```

`__consumer_offsets` 가 없으면 [archive §3](../portfolio/troubleshooting/2026-05-05-day-3-pre-entry.md#3-kafka-kraft-single-node-의-internal-토픽-생성-stuck) 절차로 docker-compose 재설정.

## fallback 정책

- API 키 미발급 / 거부 시 — fixture 기반 단위 테스트 (`uv run pytest tests/unit/`) 14 PASS 만으로 Day 2 종료 게이트 인정 (plan line 1265 / 1522)
- API rate limit 초과 시 — tenacity retry 3회 후 fail. 다음 cycle 에서 재시도. quota 회복 대기
- subway API 응답 schema 변경 시 — `_unwrap_arrival_list` + fixture + 단위 테스트 한꺼번에 갱신 (hotspot hotfix 패턴 그대로 반복). [archive §2](../portfolio/troubleshooting/2026-05-05-day-3-pre-entry.md#2-subway-url-placeholder--실시간-객차-혼잡도-api-부재)
- Kafka KRaft `__consumer_offsets` stuck 시 — `docker compose down kafka` (volume 보존) + `up -d kafka` 재생성. [archive §3](../portfolio/troubleshooting/2026-05-05-day-3-pre-entry.md#3-kafka-kraft-single-node-의-internal-토픽-생성-stuck)
- producer 메모리 / CPU 폭증 시 — Phase 1A 단계에선 단발성 디버깅 (`htop`). Phase 2 에서 `prometheus-client` 도입 검토

## 트러블슈팅 archive

### Day 2 archive — [`2026-05-02-day-2-producers-troubleshooting.md`](../portfolio/troubleshooting/2026-05-02-day-2-producers-troubleshooting.md)

| # | 이슈 | 카테고리 |
|---|------|---------|
| 1 | `tenacity` / `pyiceberg` / `pyarrow` / `apache-flink` 의존성 4중 충돌 | 의존성 / lock |
| 2 | `minio_password` 평문 노출 | 보안 / SoT 일관성 |
| 3 | URL 경로 API 키 노출 위험 | 보안 / 구조적 |
| 4 | producer flush 누락 | 데이터 안정성 / streaming |
| 5 | subway 빈 응답 가시화 | 운영 디버깅 |
| 6 | (메타) PR template 진화 + Task 단위 분할 | 프로세스 개선 |

### Day 3 진입 전 archive — [`2026-05-05-day-3-pre-entry.md`](../portfolio/troubleshooting/2026-05-05-day-3-pre-entry.md)

| # | 이슈 | 카테고리 |
|---|------|---------|
| 1 | hotspot 실 API list-wrapped `*_STTS` 호환 | 데이터 / schema |
| 2 | subway URL placeholder + 실시간 객차 혼잡도 API 부재 | 데이터 소스 의사결정 |
| 3 | Kafka KRaft single-node 의 internal 토픽 생성 stuck | 인프라 / 운영 |
| 4 | (메타) PR / commit 톤 정책 진화 | 프로세스 개선 |
| 5 | (메타) PR #15 의 의도하지 않은 결합 사고 (`git checkout -b` base 미명시) | 프로세스 / git 운영 |
| 6 | (메타) main rewrite + 이전 PR body retroactive 갱신 흐름 | 프로세스 / git 운영 |

## 레퍼런스

- Plan — `docs/superpowers/plans/phase-1a-week-1.md` Task 2.1 / 2.2 / 2.3
- Spec — `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` §6-1 Day 2, §6-2
- 이전 runbook — [`day1_infra.md`](./day1_infra.md)
