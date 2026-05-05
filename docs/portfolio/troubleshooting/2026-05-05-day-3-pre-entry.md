# Day 3 진입 전 트러블슈팅 archive (2026-05-05)

> Day 2 종료 → Day 3 진입 직전 발생한 6건 정리. 기술 4건 + 메타 2건.
>
> Day 1 archive: `2026-05-01-lakekeeper-v05-setup.md`. Day 2 archive: `2026-05-02-day-2-producers-troubleshooting.md`.
>
> 본 archive 의 6건은 Day 2 producer 의 실 API 통합 검증 시 노출된 코드 / 인프라 이슈 + 본 세션 동안 정착한 운영 컨벤션.

---

## 1. Hotspot producer 의 실 API list-wrapped `*_STTS` 호환

**관련 PR**: #14 (`fix/day-2-producer-real-api-schema`)

### 1.1. 증상

`hotspot_producer` 가 실 API 호출 시 즉시 crash:

```
File ".../src/producers/hotspot_producer.py", line 44, in parse_hotspot_payload
    pttm = live.get("PPLTN_TIME")
AttributeError: 'list' object has no attribute 'get'
```

### 1.2. 원인

실 Seoul OpenAPI 응답의 `CITYDATA.LIVE_PPLTN_STTS` (및 `ROAD_TRAFFIC_STTS`, `WEATHER_STTS`) 가 **단일 element 를 가진 list of dict** 형태로 응답. 기존 fixture 는 `dict` 단일 객체로 박혀 있어 단위 테스트는 PASS 였으나 실 API 와 schema 불일치.

### 1.3. 해결

- `_unwrap_first(x: Any) -> dict[str, Any]` helper 도입.
  - list 면 첫 원소 (dict 가정) 반환, 비-dict 첫 원소 시 `stts_unexpected_first_element` warning 로그 + `{}` 반환, dict 면 그대로 반환.
- `LIVE_PPLTN_STTS` / `ROAD_TRAFFIC_STTS` (outer + inner `AVG_ROAD_DATA`) / `WEATHER_STTS` 모두 적용.
- 키별 격리 fixture 3종 + smoke test 1종 추가 — 한 path 만 회귀 시 그 테스트만 실패.
- AVG_ROAD_DATA 도 list 가능성 미리 대응 (한 단계 안의 동일 패턴).

### 1.4. 검증

```bash
$ uv run pytest tests/unit/ -v   # 14/14 PASS
$ uv run python -m producers.hotspot_producer   # 강남역/홍대입구역(2호선)/여의도 3건 produced
$ docker exec scp-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 \
    --topic seoul.hotspot.congestion.v1
seoul.hotspot.congestion.v1:0:2
seoul.hotspot.congestion.v1:1:0
seoul.hotspot.congestion.v1:2:1   # 합계 3건 broker commit
```

### 1.5. 교훈

- 단위 테스트의 fixture 는 **실 API 응답 sample 기반** 으로 작성해야 함.
  - 추상적 mock data 는 schema 불일치를 통합 단계에서 처음 노출.
- silent fallback 회피 — list 의 첫 원소가 예상 schema 와 다를 때 `{}` 반환 + warning 로그로 운영 가시성 확보.
- 키별 격리 fixture 패턴 — "한 path 만 회귀 시 그 테스트만 실패" 가 falsifiability 확보 핵심.

### 1.6. 다른 후보 처리

- `_unwrap_first` 를 `platform_common` 모듈로 분리 검토 → 현 시점 1 producer 만 사용 → 두 번째 producer 발생 시점에 모듈화 검토.

---

## 2. Subway URL placeholder + 실시간 객차 혼잡도 API 부재

**관련 PR**: #15 (subway 변경 부분 — Kafka config 와 의도하지 않게 결합 머지, §5 별도 정리)

### 2.1. 증상

`subway_producer` 가 실 API 호출 시 `RetryError` (3회 재시도 모두 fail) → 메시지 발행 0건.

### 2.2. 원인

#### 2.2.1. 직접 원인

`SUBWAY_API_BASE = "https://openapi.seoulmetro.co.kr"` 가 placeholder.
- 코드 주석 "실제 endpoint 발급 시 교체" 가 단서.
- `seoulmetro.co.kr` 도메인은 서울교통공사 자체 사이트. 실제 OpenAPI endpoint 아님.

#### 2.2.2. 깊은 원인 — 무료 실시간 객차 혼잡도 API 부재

서울 열린데이터광장 (`swopenapi.seoul.go.kr`) 의 실시간 서비스 매트릭스 조사 결과:

| 서비스명 | 응답 | 비고 |
|---|---|---|
| `realtimeCongestion` | **404 (서비스 없음)** | 코드의 가정된 endpoint, 존재하지 않음 |
| `realtimeStationArrival` | 200 OK | **도착정보** (`btrainNo`, `arvlMsg2`, `recptnDt` 등) |
| `realtimePosition` | 200 OK (추정) | 열차 위치 |

서울 열린데이터광장의 실시간 객차 혼잡도 자체가 무료 API 로 제공 안 됨. 공공데이터포털 (data.go.kr) 의 "서울교통공사_지하철혼잡도정보" 는 30분 단위 분기별 통계 (실시간 X). 진짜 실시간 객차 혼잡도는 SK 오픈AI 등 상용 API 만 가능.

### 2.3. 해결

- `SUBWAY_API_BASE` → `http://swopenapi.seoul.go.kr` 교체.
- 서비스명 → `realtimeStationArrival`.
- `parse_subway_payload` 키셋 교체:
  - `station_code` ← `statnId`, `station_name` ← `statnNm`, `line_name` ← `subwayId`
  - `train_no` ← `btrainNo`, `direction` ← `updnLine`
  - `congestion_score` / `congestion_level` → `None` (도착정보 API 미제공 필드, 스키마 호환만 유지)
- `_unwrap_arrival_list` helper — 정상 / 데이터없음 / 에러 3가지 응답 형태 처리.
- spec / plan 의 "지하철 혼잡도" → "지하철 도착정보 + 지역 인구 혼잡도" 로 의사결정 변경 (별도 PR).

### 2.4. 검증

```bash
$ uv run python -m producers.subway_producer
INFO:httpx:HTTP Request: GET http://swopenapi.seoul.go.kr/api/subway/{key}/json/realtimeStationArrival/1/1000/ "HTTP/1.1 200 200"
2026-05-06 01:25:52 [info     ] produced_batch  count=49 station=(all)

$ docker exec scp-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 \
    --topic seoul.transit.subway.v1
seoul.transit.subway.v1:0:25
seoul.transit.subway.v1:1:28
seoul.transit.subway.v1:2:26
seoul.transit.subway.v1:3:29
seoul.transit.subway.v1:4:31
seoul.transit.subway.v1:5:22   # 합계 161건 (1 cycle, 49 batch)
```

### 2.5. 교훈

- 코드 주석 "placeholder" / "TODO" 는 통합 검증 차단 신호.
  - Day 2 종료 게이트 (실 토픽 발행 1회) 가 처음으로 placeholder 노출 — 단위 테스트만으로는 검출 불가.
- 데이터 소스 의사결정은 **실 API 명세 확인 후** 박을 것.
  - spec / plan 의 "지하철 혼잡도" 는 실 API 부재로 사실상 불가능. 의사결정 단계에서 endpoint 검증 우선.
- 지역 단위 분석 시 인구 혼잡도 (도시데이터 API) + 도착정보 (지하철 API) 결합이 객차 혼잡도 부재를 보완.

### 2.6. 다른 후보 처리

| 후보 | 처리 |
|---|---|
| 분기별 혼잡도 통계 (`서울교통공사_지하철혼잡도정보`, data.go.kr) | Phase 2 batch 검토 (실시간 X, streaming 의도와 충돌) |
| 상용 실시간 객차 혼잡도 (SK 오픈AI 등) | 본 프로젝트 비용 0원 목표와 충돌, 제외 |
| 2025-07 서울교통공사 신규 개방 데이터 | data.go.kr 카탈로그 별도 점검 시점에 검토 |

---

## 3. Kafka KRaft single-node 의 internal 토픽 생성 stuck

**관련 PR**: #15 (Kafka config 부분)

### 3.1. 증상

- Day 2 hotspot 토픽에 broker offset 으로 메시지 3건 commit 확인됨에도 console-consumer 가 timeout (0 메시지 read).
- broker 로그:
  ```
  INFO Sent auto-creation request for Set(__consumer_offsets) to the active controller.
  ```
  를 1초 간격으로 무한 retry.

### 3.2. 원인

KRaft single-node 환경 (broker 1개) 에서 internal 토픽 (`__consumer_offsets` / `__transaction_state`) 의 default replication factor 가 **3**.
- broker 1개로는 RF=3 충족 불가 → controller 가 토픽 생성 거부 → broker 가 retry 무한 반복.
- consumer group join 불가 → console-consumer / PyFlink consumer 모두 동작 안 됨.

추가 발견 — `docker compose stop kafka -t 30 + start` 만으로는 broker session lease 만료가 진행 안 됨. KRaft 의 controller 와 broker 가 같은 프로세스라 controller 도 같이 stop → broker session timeout cleanup 진행 안 됨. `DUPLICATE_BROKER_REGISTRATION` 로그가 새 broker 등록 거부.

### 3.3. 해결

`docker-compose.yml` 의 kafka 환경변수에 추가:

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

재기동 절차 — `docker compose down kafka` (volume 보존) → `docker compose up -d kafka` → broker startup wait 15초.

### 3.4. 검증

```bash
$ docker logs scp-kafka --tail 30 --since 60s | grep auto-creation   # 출력 없음 (retry stuck 사라짐)
$ docker exec scp-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list | grep __consumer_offsets
__consumer_offsets   # 자동 생성 확인

$ docker exec scp-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic seoul.hotspot.congestion.v1 \
    --from-beginning --max-messages 1 --timeout-ms 30000
schema_version:v1,api_response_ts:2026-05-06T00:05:00,...   POI001   {"area_code":"POI001",...}
Processed a total of 1 messages
```

### 3.5. 교훈

- single-node 결정의 운영 정합성은 internal 토픽까지 영향.
  - default 가 multi-broker 가정인 항목 (RF=3, MIN_ISR=2 등) 은 single-node 에서 명시 override 필요.
- broker 측 commit (offset) 과 consumer 측 read 는 별개 검증.
  - producer 의 `produced` 로그만으로 broker ack 확신 어려움 — `kafka-get-offsets.sh` 또는 console-consumer 로 broker / consumer 양측 확인.
- KRaft 의 broker session lease 는 controller 가 살아있어야 만료 진행.
  - single-node 에서 stop + start 만으로는 stuck 해소 어려움 — `docker compose down + up` (컨테이너 재생성) 권장.

### 3.6. 다른 후보 처리

- KRaft volume 초기화 (메시지 손실) — Day 1 인프라 셋업 절차 다시. `down + up` 으로 해결되면 회피 가능.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` 변경 — internal 토픽 stuck 의 root cause 가 RF 라 auto-create 활성화는 무관 + 일반 토픽 난립 위험.

---

## 4. PR / commit 톤 정책 진화

**관련 메모리**: `korean-conventions.md` (지속 갱신).

### 4.1. 증상

PR / commit body 에서 다음 부류 표현이 광범위 발견됨:
- "면접 / 회고 / 면접관" 같은 채용 어필 표현
- "포트폴리오" / "1번 포트폴리오" 같은 외부 컨텍스트 노출
- "narrative" / "어필" / "회고 자료" 같은 외래어 / 메타 표현
- "사용자가 ~" 같은 외부 화자 화법
- "~습니다" / "~합니다" 같은 격식 종결어미
- em dash (`—`) 부연 설명이 한 줄에 너무 길게 박혀 가독성 저하

### 4.2. 원인

- 초기 PR / commit 작성 시 톤 정책이 메모리에 부분적으로만 박혀 있었음 (`(portfolio 어필)` 직접 표현 금지 정도).
- 광범위 적용 가이드 + 자주 쓰는 외래어 / 메타 표현 대체어 명시 부족.

### 4.3. 해결

`korean-conventions.md` 메모리에 다음 정책 정착:
- **금지 표현 매트릭스** — 면접 / 포트폴리오 / 어필 / narrative / 회고 / 사용자가 / 1번 포트폴리오 / JD / 이력서 / 취업 / silent data loss / schema drift / defensive / falsifiability / atomicity / round 1 / round 2 / code review 에서 / reviewer 가 / implementer 가 모두 금지.
- **종결어미 정책** — `~습니다` / `~합니다` / `~됐습니다` / `~입니다` 격식 종결 사용 금지. 명사형 / `~함` / `~음` / 짧은 평서형 `~다` 권장.
- **Em dash 정책** — 부연 설명 시 줄바꿈 또는 sub-bullet 으로 분리. 짧은 보충 / 병기 / 구분자 용도는 OK.
- **외래어 풀어쓰기** — silent data loss → "데이터 누락 발생", schema drift → "API 응답 형태 변경", defensive → "미리 방어 처리", falsifiability → "회귀 시 어느 path 가 깨졌는지 골라낼 수 있는 구조" 등.

### 4.4. 검증

이번 세션 동안 PR #14 + PR #15 본문 + 5 commit body 모두 위 정책 적용 확인 (grep 잔여 0건).

### 4.5. 교훈

- 톤 정책은 **명문화 후 일관 적용**.
  - 부분적 메모리만으로는 광범위 적용 어려움.
- 격식 종결어미 (`~습니다`) 는 회의 보고서 톤. 실무 메모는 명사형 / 짧은 평서가 정보 밀도 ↑.
- Em dash 부연 설명은 nested bullet 으로 분리해야 reviewer 가 빠르게 스캔 가능.
- 외래어 jargon 은 reviewer 의 이해 부담 ↑. 한국어 풀어쓰기 우선.

### 4.6. 다른 후보 처리

- CONTRIBUTING.md / `.github/PULL_REQUEST_TEMPLATE.md` 에 본 정책 명문화 — 별도 PR 검토 (현재는 메모리 SoT).
- 자동화 — pre-commit hook 으로 commit message grep 검증 — Phase 2 W4 이후 검토.

---

## 5. PR #15 의 의도하지 않은 결합 사고 (git checkout -b base 미명시)

### 5.1. 증상

PR #15 의 squash merge commit `ea71298` 가 docker-compose.yml + subway_producer.py + subway fixture / test 모두 변경. 의도는 `docker-compose.yml` 1 파일만이었으나 실제로는 4 파일 결합 머지.

### 5.2. 원인

- `git checkout -b fix/day-1-kafka-single-node-replication` 실행 시 base branch 미명시.
- 직전 branch 가 `fix/day-2-subway-real-endpoint` (subway hotfix 의 implementer 작업 후 상태) 였음.
- `git checkout -b NEW` 는 직전 branch HEAD 위에 새 branch 를 갈라낸다.
- 결과 — fix/day-1-... 의 base 가 subway hotfix HEAD (42294ed) 가 되어 PR #15 의 commit list = [5fa803b, 42294ed, ae58c73] 3개.
- squash merge 가 3개를 합쳐 main 의 단일 commit (ea71298) 으로 머지.

reflog 단서:
```
HEAD@{7}: checkout: moving from fix/day-2-subway-real-endpoint to fix/day-1-kafka-single-node-replication
```

### 5.3. 해결

- 결과적으로 main 의 코드는 정확 (Kafka config + subway endpoint 둘 다 유효한 fix).
- PR #15 본문에 사고 흐름 §2.4 추가 + 트러블슈팅 archive cross-link.
- subway hotfix branch 삭제 (redundant, rebase 시 patch already upstream).
- main rewrite 회피 (비용 큼) — 한 PR = 한 논리 작업 위반은 인정.

### 5.4. 검증

- main 의 ea71298 변경 = docker-compose.yml + subway 4 파일 (의도하지 않은 결합 그대로).
- 14/14 단위 테스트 PASS.
- hotspot + subway 양쪽 토픽 실 메시지 발행 확인.

### 5.5. 교훈

- 새 brunch 시작 시 항상 `git switch -c <new> main` 또는 `git checkout main && git checkout -b <new>` 로 base 명시.
- Subagent dispatch 후 main 으로 복귀 확인 절차 추가.
- squash merge 직전 PR commit list (`gh pr view <n> --json commits`) 확인 절차 추가.

### 5.6. 다른 후보 처리

| 후보 | 채택 안 한 이유 |
|---|---|
| main rewrite + force push | 새 hash, 모든 reference 또 갱신, 비용 대비 효과 낮음 |
| revert ea71298 + 별도 PR 로 다시 머지 | commit history 에 revert + 재머지 noise |

---

## 6. main rewrite + 이전 PR body retroactive 갱신 흐름

### 6.1. 증상

이전 PR #1~12 본문 + main 의 squash commit body 광범위 영역에서 면접 / 포트폴리오 / narrative 등 금지 표현 발견. 톤 정책 정착 시점 이전이라 cutoff vs retroactive 갱신 결정 필요.

### 6.2. 원인

- 톤 정책 (금지 표현 + 종결어미 + em dash) 정착 이전 작성된 PR body.
- 단순 sed 변환은 의미 깨짐 위험.
- main 의 squash commit body 갱신은 main rewrite 트리거.

### 6.3. 해결

Phase A → B → C → D 단계로 일괄 처리:

| Phase | 작업 | 도구 |
|---|---|---|
| A | PR #13 (현재 작업, 머지 전) 본문 + commit amend | `git commit --amend` + `gh pr edit --body-file` + force-with-lease push |
| B | 이전 PR #2~12 body 갱신 (10건) | Python regex substitution + placeholder swap (`docs/portfolio/` 등 보존) + `gh pr edit --body-file` |
| C | main 12 squash commit body rewrite | `git filter-branch --msg-filter` + 동일 substitution + 단어 사이 다중공백 lookaround 압축 (`(?<=\S)  +(?=\S) → ` `) + force-with-lease push |
| D | 메모리 4 위치 + PR body 1 위치 hash reference 갱신 | Edit + gh pr edit |

### 6.4. 검증

- 잔여 금지 표현 grep 결과 0건 (main + 11 PR body).
- 다중공백 0건 (단어 사이 only, indent 보존).
- backup branch (`backup-main-pre-rewrite`) 로 사고 시 즉시 롤백 가능.
- GitHub PR merged 상태 보존 확인 (squash commit hash 변경에도 PR 자체는 closed-merged).

### 6.5. 교훈

- 메모리 정책의 "시점 cutoff 원칙" 도 옵션 — retroactive 갱신 vs cutoff 보존은 사용자 결정.
- 다중공백 압축 시 lookaround 로 indent 보존 필수.
  - 단순 `r' {2,}': ' '` 은 markdown nested bullet 의 indent 까지 깸.
- 디렉토리명 / brunch명 (`docs/portfolio/...`, `phase-1a/day-10-portfolio`) 은 placeholder swap 으로 보존.
- main rewrite 후 stale hash reference (메모리 / PR body) 는 sed 일괄 갱신 가능.

### 6.6. 다른 후보 처리

- cutoff 만 적용 (옛 PR body / commit message 그대로 보존) — 간단하지만 일관성 ↓.
- 자동화 hook (commit msg-filter) 로 초기부터 정책 강제 — Phase 2 검토.

---

## 부록 — Day 3 진입 직전 체크리스트

- [x] PR #13 (Spark 4 + JDK 17 결정) 머지
- [x] PR #14 (hotspot real API hotfix) 머지
- [x] PR #15 (Kafka config + subway endpoint, 결합 머지) 머지
- [x] hotspot + subway 양쪽 토픽 실 메시지 발행 확인 (broker offset)
- [x] `__consumer_offsets` 자동 생성 + console-consumer 정상 read 확인
- [ ] spec/plan 갱신 PR (혼잡도 → 도착정보 + 인구 혼잡도) — 별도 진행
- [ ] Day 2 runbook 갱신 (Kafka 운영 + subway endpoint 변경) — 별도 진행
- [ ] 메모리 갱신 (새 main HEAD + Day 3 진입 절차) — 별도 진행

Day 3 (PyFlink Bronze→Silver) 는 새 세션에서 진입.
