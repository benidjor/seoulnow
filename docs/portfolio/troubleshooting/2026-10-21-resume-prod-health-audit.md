# 프로젝트 재개 점검 — prod 부분 장애 · 도메인 미연결 · area_code 매핑 오류

**발생일**: 2026-10-21 (Phase 1B Day 11 이후 약 5개월 쉬었다가 재개하며 한 점검)
**관련 runbook**: [`prod-health-check.md`](../../runbook/prod-health-check.md) / [`vm-migration-mvp.md`](../../runbook/vm-migration-mvp.md) / [`phase-1b-prep/03-cloudflare-pages-monorepo.md`](../../runbook/phase-1b-prep/03-cloudflare-pages-monorepo.md)
**관련 spec**: `docs/superpowers/specs/2026-05-25-vm-migration-design.md`
**기준 commit**: main `a89f5af` (2026-05-26 이후 commit 없음)

## 요약

작업을 재개하며 외부에서 계층별로 점검해 보니 운영 중이라고 기록된 상태와 실제 상태가 달랐다.
이슈 3건을 발견했다.
Issue 1은 VM 내부 점검이 아직 남아 있어 원인을 확정하지 못했고, Issue 2 · 3은 원인을 확정했다.

| # | 이슈 | 클래스 | 상태 |
|---|---|---|---|
| 1 | 페이지는 200인데 데이터 API가 504 → 빈 지도 | 운영 (always-on 유지보수 부재) | 원인 추정, VM 점검 대기 |
| 2 | `seoulnow.live` apex가 처음부터 동작한 적 없음 | 이월 작업 누락 | 원인 확정, 대시보드 조치 대기 |
| 3 | 핫스팟 `area_code`가 공식 `AREA_CD`와 불일치 | 데이터 정합성 | 원인 확정, 코드 수정 대기 |

덧붙여 같은 API 응답 안에 지금 쓰지 않는 블록 (12시간 예측, 업종별 결제 활발도, 승하차 인원 등)도 발견했다 → [부록](#부록--citydata-응답-구조-실측).

---

## Issue 1 — 페이지는 200인데 데이터 API가 504 → 빈 지도

### 증상

| 대상 | 결과 |
|---|---|
| `https://seoulnow.pages.dev/` | 200 (0.14초) |
| `https://seoulnow.pages.dev/api/v1/hotspots` | 504, `{"items":[]}` (약 4초) |
| `https://seoulnow.pages.dev/api/v1/chill-open-now` | 504, `{"snapshot_ts":null,"places":[]}` |
| `https://api.seoulnow.live/` | 404 (1초) |
| `https://api.seoulnow.live/api/hotspots` | 30초, 60초 모두 응답 없음 |

### 원인

계층을 하나씩 떼어 확인하면 장애가 난 곳은 **FastAPI가 Iceberg gold 테이블을 읽는 구간**이다.

- Pages 200 → 프론트 배포는 정상
- Pages Functions 504 → Edge가 upstream 타임아웃 뒤 degraded 응답을 반환 (설계대로 동작)
- `api.seoulnow.live/` 404가 바로 돌아옴 → Cloudflare Tunnel + VM + FastAPI 프로세스는 살아 있음 (루트 라우트가 없을 뿐)
- `/api/hotspots`만 무응답 → 라우트 내부의 데이터 조회가 끝나지 않음

**추정 원인 (VM 미점검)**: gold 테이블에 small file이 쌓였다.

- serving 쿼리 (`src/api/routes/hotspots.py`)는 요청마다 `pyiceberg plan_files()`로 **gold 전체 파일 목록**을 만들고 DuckDB `read_parquet`로 전부 스캔한 다음 최신 window만 고른다.
- VM MVP는 Airflow 없이 띄웠다.
  그래서 `iceberg_maintenance` compaction이 한 번도 돌지 않았다.
  VM spec §7은 compaction을 "always-on prod 필수"로 적어 두고도 2차 작업 (VM 에 Airflow 를 다시 올리는 작업)으로 미뤘다.
- Day 11 Task 11.0 사전 조사 때 이미 파일 9,161개 상태에서 읽기에 5~29초가 걸리는 것을 실측했다.
  5분 tumbling window가 계속 commit하므로 약 5개월이 지난 지금은 파일 · snapshot이 그보다 훨씬 많다.
- 그 밖에 디스크 포화 (50GB), Flink job 중단, 메모리 부족도 배제하지 못했다 → [runbook §3](../../runbook/prod-health-check.md)으로 확인해야 한다.

### 해결 (예정)

1. VM 상태 점검: `df -h` / `free -h` / systemd 4종 / gold 파일 · snapshot 수
2. Spark compaction (`infra/spark/jobs/compaction_gold.py`) + `expire_snapshots` 수동 실행
3. 재발 방지: VM에 compaction systemd timer 등록 (VM 에 Airflow 를 올리기 전까지 임시)
4. serving 쿼리 범위 제한: 최근 partition · snapshot만 읽도록 수정

### 교훈

- **"MVP 먼저, 유지보수는 2차"라는 결정은 always-on 시스템에서 시한폭탄이 된다.**
  streaming sink가 5분마다 commit하므로 compaction은 MVP 범위에 넣었어야 했다.
- **외부 헬스체크가 없었다.**
  5개월 동안 장애를 아무도 몰랐다.
  데이터 API 응답 시간을 주기적으로 확인하는 장치가 필요하다.

---

## Issue 2 — `seoulnow.live` apex가 처음부터 동작한 적 없음

### 증상

```
$ curl https://seoulnow.live/
curl: (6) Could not resolve host: seoulnow.live
```

| 확인 항목 | 결과 |
|---|---|
| 도메인 등록 (whois) | `ACTIVE` (Namecheap, 2026-05 구매) |
| NS | `kenia.ns.cloudflare.com` / `sean.ns.cloudflare.com` → Cloudflare 위임 정상 |
| `api.seoulnow.live` | 정상 resolve (Tunnel CNAME) |
| `seoulnow.live` A / AAAA / CNAME | **레코드 없음** |
| `www.seoulnow.live` | 레코드 없음 |

### 원인

- 사전 준비 runbook `03-cloudflare-pages-monorepo.md` §6이 Pages Custom domain 설정을 **"Day 11 implementation 시점 작업"**으로 미뤘다.
- Day 11 (PR #73~#78)과 VM 마이그레이션 동안 모든 검증을 `*.seoulnow.pages.dev`로 했다. 그래서 apex가 없어도 문제가 드러나지 않았다.
- `vm-migration-mvp.md` §3에 "Pages 재배포 → https://seoulnow.live 지도 실데이터"라고 적혀 있었다. 실제로 검증한 주소는 pages.dev였으므로 기록이 사실과 달랐다 (이번 점검 때 정정).

### 해결 (예정 — 대시보드 작업)

Cloudflare dashboard → `Workers & Pages` → `seoulnow` → `Custom domains` → `Set up a domain` → `seoulnow.live`. zone이 같은 계정에 있으므로 apex CNAME (flattening)이 자동으로 잡히고 인증서도 자동 발급된다. 필요하면 `www`도 추가하거나 리다이렉트한다.

### 교훈

- **뒤로 미룬 작업은 다음 단계에 들어가기 전 확인하는 체크리스트에 적어야 한다.** runbook 본문에만 남은 "나중에 할 일"은 잊힌다.
- 검증 기록에는 **실제로 확인한 URL**을 그대로 적는다 (의도한 URL을 적지 않는다).

---

## Issue 3 — 핫스팟 `area_code`가 공식 `AREA_CD`와 불일치

### 증상

이름과 코드로 각각 직접 조회해 보니 다음과 같았다.

| 조회 | 응답 `AREA_NM` | 응답 `AREA_CD` |
|---|---|---|
| `citydata/1/5/강남역` | 강남역 | **POI014** |
| `citydata/1/5/POI001` | 강남 MICE 관광특구 | POI001 |

그런데 `data/reference/hotspot_regions.csv`는 `POI001,강남역,...`으로 매핑한다.

### 원인

- `hotspot_producer.py`는 `DEFAULT_AREAS = {"POI001": "강남역", ...}`의 **이름**으로 API를 조회한다. 그러고 나서 응답의 `AREA_CD`가 아니라 dict의 **키**를 `area_code`로 붙여 발행한다.
- 이 코드 체계는 Day 2~3에 임의로 정했고 공식 장소 목록과 대조한 적이 없다.
- 홍대입구 (POI002), 여의도 (POI003) 등 나머지 매핑도 틀렸을 가능성이 높다.

### 영향

- 파이프라인 내부 (Kafka key → bronze partition → silver region join)는 같은 CSV를 쓰기 때문에 일관되게 동작했다. 그래서 오류가 드러나지 않았다.
- 하지만 공식 장소 목록, 장소 영역 데이터, 다른 서울시 데이터와 join하면 엉뚱한 장소에 붙는다. 핫스팟을 늘리면 코드가 충돌한다 (공식 POI001이 들어오는 순간 강남역과 겹친다).

### 해결 (예정)

1. producer가 응답의 `AREA_CD`를 그대로 `area_code`로 쓴다
2. `hotspot_regions.csv`를 공식 장소 목록 (최근 보도 기준 116곳)으로 다시 작성한다
3. 기존 bronze/silver/gold의 코드를 변환하거나 재적재 범위를 정한다 (VM 데이터는 재적재로 만든 것이므로 다시 적재하는 편이 단순하다)

### 교훈

- **외부 식별자는 외부 시스템이 준 값을 그대로 쓴다.** 조회 키 (이름)와 식별자 (코드)를 클라이언트 쪽에서 짝지으면, 짝이 틀려도 내부에서는 계속 일관되어 보인다.

---

## 부록 — citydata 응답 구조 실측

2026-10-21에 강남역 응답을 확인했다. 현재 producer는 `LIVE_PPLTN_STTS` 일부 (혼잡도 · 인구)와 도로 · 날씨 일부만 쓴다.

| 블록 | 주요 필드 | 활용 가능성 |
|---|---|---|
| `LIVE_PPLTN_STTS` | `AREA_CONGEST_LVL`, 인구 min/max, 성별 · 연령대 · 거주 비율, `REPLACE_YN`, `PPLTN_TIME` | `REPLACE_YN` = 대체 데이터 플래그 (품질 검증) |
| `LIVE_PPLTN_STTS.FCST_PPLTN` | 12개 시간 슬롯 × (`FCST_TIME`, `FCST_CONGEST_LVL`, 인구 min/max) | "1시간 뒤에도 한가" 추천 |
| `LIVE_CMRCL_STTS` | 상권 활성도, 결제 건수 · 금액, `CMRCL_TIME` | 지역 소비 활동 |
| `LIVE_CMRCL_STTS.CMRCL_RSB` | 업종 대 · 중분류 × 결제 활발도 (한산한/보통/분주한), 결제 건수 | "제과/커피/패스트푸드", "유흥" 업종 → 카페 · 술집 혼잡에 가까운 신호 |
| `LIVE_SUB_PPLTN` / `LIVE_BUS_PPLTN` | 5 · 10 · 30분 이내 승하차 인원, 누적 | 유입 추세 (혼잡 선행 지표) |
| `WEATHER_STTS` | 기온, 강수, 미세먼지, `FCST24HOURS` | 맥락 피처 |
| `EVENT_STTS` / `SBIKE_STTS` / `PRK_STTS` / `CHARGER_STTS` / `ROAD_TRAFFIC_STTS` | 행사, 따릉이, 주차장, 충전소, 도로 소통 | 맥락 피처 |

응답 구조를 직접 확인하는 명령은 [runbook §4](../../runbook/prod-health-check.md)에 있다. 데이터 소스를 어떤 순서로 늘릴지는 별도 안건으로 다룬다.
