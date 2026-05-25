# VM 마이그레이션 설계 — prod 데이터 플랫폼을 Oracle Cloud VM 으로

> 작성 2026-05-25. 본 문서는 "Mac 로컬에서 구축된 Phase 1A/1B 데이터 플랫폼을 Oracle Cloud Always Free VM 으로 prod 이전" 의 단일 출처(SoT).
> Phase 1 설계(`2026-04-30-...phase1-design.md`) + CLAUDE.md §5(인프라)·§13(메모리 제약) 의 하위. 본 문서는 그 위에서 "어디서 돌리나(host)" 결정만 다룬다.

---

## 0. 배경 — 왜 지금

설계(CLAUDE.md §5: `컴퓨트 = Oracle Cloud Always Free VM 24GB`, Phase 1 spec §6-1 Day 1: `Oracle VM + docker-compose`)는 처음부터 **전체 파이프라인의 prod 호스트 = Oracle VM** 으로 의도했다. 그러나 실제 Phase 1A/1B 는 **Mac 로컬**에서 구축·가동됐고(dev 편의), VM 은 Day 11 prep 때 **HTTP receiver(:8400) + cloudflared tunnel 용도로만** provisioning 됐다. 결과:

- gold 데이터(로컬 MinIO `seoul-warehouse`, 객체 27k)는 **Mac 에만** 존재. VM 에는 Kafka/MinIO/gold 가 없음.
- 그래서 배포 지도(`*.seoulnow.pages.dev`)가 실데이터를 못 보여주고 degraded 배너 표시.
- VM 의 `receiver.seoulnow.live` tunnel 은 origin(receiver + Kafka)이 VM 에 없어 사실상 **stranded**(502).

**본 마이그레이션이 이 갭을 닫는다** — prod(VM)에 데이터 플랫폼을 세워 공개 데모를 24/7 서빙하고, 이미 만든 VM tunnel 을 설계대로 작동시킨다.

---

## 1. 목표 / 성공 기준

- **VM = prod(항상 켜짐), Mac = dev(전체 스택 유지)** — 두 독립 환경(업계 표준 dev/prod 분리).
- **MVP 성공 기준**: `https://seoulnow.live` 지도가 **VM 에서 서빙되는 실데이터**(혼잡도 색상 + chill-open 마커)를 표시 + degraded 배너 소멸 + **노트북이 꺼져도 유지**.
- 재적재(re-ingest) 방식 — historical 은 Mac 에만, VM 은 "지금부터" 실시간 데이터.

---

## 2. 핵심 결정 (brainstorm 2026-05-25)

| # | 항목 | 결정 | 사유 |
|---|------|------|------|
| 1 | Mac/VM 관계 | **VM = prod(always-on), Mac = dev(전체 스택 유지)** 2 독립 환경 | dev/prod 분리 표준, VM 24GB 를 개발 부하로부터 보호 |
| 2 | VM 데이터 적재 | **신규 재적재(re-ingest)** — 빈 스택 + 인허가 CSV + producer 폴링으로 "지금부터" 재생성 | 단순·재현 가능, historical 은 Mac 에 있음, compaction/SLO 증거는 Mac 에서 측정 완료 |
| 3 | 호스트 프로세스 관리 | **컴포넌트별 systemd 유닛**(스트리밍·producer·serving), 인프라는 docker-compose 유지 | Mac 과 동일 모델(host process, `MINIO_ENDPOINT=localhost`로 pyiceberg S3 alias 이슈 회피) + 부팅 영속 + 기존 cloudflared systemd 와 일관 |
| 4 | 스코프/순서 | **MVP(데모 임계경로) 먼저 → 레이어링 후속** | 데모를 빠르게 live, 24GB 점진 관리. 최종은 전체 패리티 |
| 5 | 지속 배포 | **GitHub Actions ssh 자동 배포**(main push → ssh → git pull + 영향 systemd/compose 반영) | `[[deployment-automation-first]]` 정합(ssh+수동 회피) |
| 6 | 공공 API 쿼터 격리 | **VM 전용 키 신규 발급** — VM 은 새 `SEOUL_OPENAPI_KEY`(MVP)·`SEOUL_SUBWAY_API_KEY`(2차), Mac 은 기존 키 | 두 환경 동시 폴링 시 일일 쿼터 2배 소비 → 한도 초과 시 양쪽 동반 실패 회피. 값 = `.local-notes/vm-secrets.md`(미커밋). 인허가는 CSV 라 키 불필요 |
| 7 | VM Airflow meta(2차) | **SQLite + LocalExecutor 로 시작**, 메모리 여유 확인 후 Postgres-meta 승격 옵션(이력 리셋 감수) | CLAUDE.md §13 정합(~700MB), 24GB 절약. 승격은 conn string 변경 + `airflow db migrate` 수준 |

---

## 3. 타깃 아키텍처 (VM, 155.248.164.17 — Ubuntu 24.04 aarch64 / 4 OCPU / 24GB / 50GB)

```
[인프라 — docker-compose]
  kafka(apache/kafka:4.0.0) · postgres:16 · minio · lakekeeper(v0.12.1)
  [+ 레이어링] kafka-connect(debezium/connect:2.7.3.Final) · airflow(SQLite+LocalExecutor)

[호스트 프로세스 — systemd]
  hotspot_producer → bronze_to_silver → silver_to_gold → (dbt mart)
  serving FastAPI(:8000)
  http-receiver(:8400, compose profile=receiver 유지)
  [+ 레이어링] subway_producer · cdc_to_dim_place · slo_metrics

[노출 — cloudflared(기존 tunnel seoulnow-receiver 재사용, outbound only, ufw 22 만 inbound)]
  api.seoulnow.live      → http://localhost:8000   (신규 ingress)
  receiver.seoulnow.live → http://localhost:8400   (기존 ingress — 비로소 작동)

[Cloudflare Pages env]
  CHILL_API_BASE=https://api.seoulnow.live
  EVENTS_RECEIVER_BASE=https://receiver.seoulnow.live
  RECEIVER_TOKEN, ANON_UA_SALT
```

데이터 흐름(MVP): 공공 citydata API → hotspot_producer(Kafka `seoul.hotspot.congestion.v1`) → bronze_to_silver → silver_to_gold(Iceberg gold `fact_hotspot_congestion_5min`) → dbt mart `chill_open_now` → serving FastAPI(DuckDB read: `/api/hotspots` = gold 자치구 집계, `/api/chill-open` = chill_open_now) → Pages Edge route → 브라우저 지도.

> 주의: `congestion_grade_5min` mart 는 Phase 1B Day 17 산출물이라 MVP 시점엔 없음 — 혼잡도 등급은 serving 이 `fact_hotspot_congestion_5min` 에서 직접 산출(Task 11.0 현행과 동일).

---

## 4. MVP 스코프 (1차) — 데모 임계경로

순서:

1. **부트스트랩(수동 runbook)**: docker + compose plugin 설치 → repo clone(또는 기존 `~/seoulnow/` 갱신) → **`.env` 전체 sync**(API 키는 VM 전용 키로 치환 — `.local-notes/vm-secrets.md`, minio/postgres creds 등) → ARM 멀티아치 이미지 pull 검증 → 인프라 compose 기동(kafka/postgres/minio/lakekeeper) + healthcheck.
2. **데이터 재적재**: `load_static_places.py`(인허가 CSV) 적재 → `hotspot_producer` systemd 시작 → bronze→silver→gold systemd 시작 → 1 cycle(5분) 후 gold `fact_hotspot_congestion_5min` 적재 확인 → `dbt run`(현 dbt 프로젝트 mart — 최소 `chill_open_now`) + `dbt test`.
3. **serving + 노출**: serving FastAPI systemd 기동(:8000) → tunnel config 에 `api.seoulnow.live → :8000` ingress 추가 + DNS route + cloudflared restart → Pages env(`CHILL_API_BASE` 등) 설정 → 재배포.
4. **검증**: `https://seoulnow.live` 지도 실데이터 표시 + degraded 소멸 + 노트북 종료 후에도 유지.
5. **배포 자동화 구축**: `.github/workflows/deploy-vm.yml`(ssh deploy) 세팅 + 1회 테스트 push 반영 확인.

---

## 5. 레이어링 스코프 (2차 — 별도 plan/Day)

- `subway_producer` + subway 파이프라인(VM 지하철 키 사용)
- CDC: kafka-connect(Debezium) + `cdc_to_dim_place`
- **Airflow(SQLite+LocalExecutor)** — 특히 `iceberg_maintenance` compaction(L129상 small-file 연속 누적 → always-on prod 필수) + `slo_daily_report` + `dbt_full_run` + `backfill_silver_from_bronze`
- `slo_metrics` flink job
- events receiver end-to-end(Task 11.1-B 트랙 A·B 머지 연동)

---

## 6. 프로세스 관리 (systemd)

각 유닛 = boot 자동 시작 + 죽으면 자동 재시작(`Restart=always`) + journald 로그. `WorkingDirectory=~/seoulnow`, `Environment=MINIO_ENDPOINT=http://localhost:9000`(Mac 과 동일, pyiceberg PyArrow S3 의 /etc/hosts alias 이슈 회피), `ExecStart=uv run python -m <module>`.

| 유닛 | 모듈 | 단계 |
|---|---|---|
| `seoulnow-hotspot-producer` | `producers.hotspot_producer` | MVP |
| `seoulnow-bronze-silver` | `flink_jobs.bronze_to_silver` | MVP |
| `seoulnow-silver-gold` | `flink_jobs.silver_to_gold` | MVP |
| `seoulnow-api` | `uvicorn api.main:app --port 8000`(run_api.sh 기반) | MVP |
| `seoulnow-subway-producer` | `producers.subway_producer` | 레이어링 |
| `seoulnow-cdc-dim-place` | `flink_jobs.cdc_to_dim_place` | 레이어링 |
| `seoulnow-slo-metrics` | `flink_jobs.slo_metrics` | 레이어링 |

http-receiver 는 docker-compose `profile=receiver` 서비스 그대로 유지(systemd 아님).

---

## 7. 배포 자동화 (GitHub Actions ssh)

`.github/workflows/deploy-vm.yml`:
- 트리거: `push` to `main`(경로 필터로 frontend-only 변경 제외 가능).
- 동작: ssh(키 = GH secret `VM_SSH_KEY`, host/user = secret) → `cd ~/seoulnow && git pull` → 변경 감지 → 영향 systemd 유닛 `sudo systemctl restart <unit>` / compose 변경 시 `docker compose up -d`.
- 첫 부트스트랩은 본 워크플로우 이전(수동 runbook)에 1회 완료 전제 — Actions 는 **이후 갱신만** 담당.

---

## 8. 제약 / 리스크

| 항목 | 내용 / 대응 |
|---|---|
| **24GB 메모리 예산** | MVP(kafka+postgres+minio+lakekeeper + flink 2 + producer 1 + api)는 여유. 레이어링 시 Airflow=SQLite(§13, ~700MB) + kafka-connect 추가. 도입 직후 `free -h` 측정(80% 임계 = 19.2GB) |
| **50GB 디스크** | 재적재라 데이터 작음 + docker 이미지 수 GB → 여유. 장기 누적은 compaction DAG(2차)가 관리 |
| **ARM 멀티아치** | 전 이미지 arm64 pull 가능 여부를 plan 첫 step 에서 검증(과거 bitnami retired / Lakekeeper 셋업 이슈 기왕력 — `[[env-deviations-day-1]]`). 실패 시 대체 이미지/태그 |
| **공공 API 쿼터** | VM 전용 키로 격리(결정 #6). Mac dev 와 일일 쿼터 독립 |
| **secrets sync** | VM `~/seoulnow/.env` = Mac `.env` 기반 + API 키만 VM 전용 키로 치환. 현재 VM 엔 `RECEIVER_TOKEN` 만 박힘 → 나머지(citydata 키·minio/postgres creds·`ANON_UA_SALT` 등) 추가 필요. 전송은 ssh pipe(값 노출 0) |
| **노출 최소화** | cloudflared outbound only, ufw inbound 22 만. 내부 서비스(kafka/minio/postgres/lakekeeper)는 localhost 바인딩 |
| **이중 운영 혼동** | Mac·VM 양쪽이 같은 토픽명·버킷명 사용하나 **물리적으로 분리된 환경**(서로 다른 Kafka/MinIO/키). 혼동 방지를 위해 runbook 에 "어느 환경인지" 명시 |

---

## 9. Out of scope (본 마이그레이션 제외)

- Superset(Phase 1B Day 17) / Trino(Day 18) — 추후 별도.
- Spark(Day 9 on-demand, `profile=spark`) — VM 일시 기동 패턴 유지, 상시 아님.
- Mac 스택 폐기 — Mac 은 dev 로 계속 유지(결정 #1).
- historical 데이터 VM 이전 — 재적재라 미수행(결정 #2).

---

## 10. 검증 체크리스트

**MVP 완료 게이트:**
- [ ] VM 인프라 compose 4서비스 healthy(kafka/postgres/minio/lakekeeper)
- [ ] VM gold `fact_hotspot_congestion_5min` 에 데이터 적재(DuckDB 확인) + dbt mart PASS
- [ ] `seoulnow-*` systemd 유닛 active + enabled(boot 영속)
- [ ] `curl https://api.seoulnow.live/api/hotspots` → 200 + 실데이터
- [ ] `https://seoulnow.live` 지도 실데이터 표시 + degraded 배너 소멸
- [ ] **노트북 종료 후에도 지도 유지**(prod 독립성 핵심 검증)
- [ ] `.github/workflows/deploy-vm.yml` 1회 테스트 push 반영 확인

**레이어링 완료 게이트(2차):** subway + CDC + Airflow(compaction/slo) + receiver end-to-end + `free -h` 19.2GB 하회.
