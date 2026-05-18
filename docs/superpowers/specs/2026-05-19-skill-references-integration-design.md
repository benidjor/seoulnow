# k-skill / cafefinder reference 통합 + Day 17 임계값 정정 — Design

> 2026-05-19 brainstorm 세션 (`superpowers:brainstorming`) 결과의 영속화. 본 PR (`docs/skill-references-integration`) 의 source of design. Section 1-6 결정 trace + Section 7 종료 game plan.
>
> 본 design 의 implementation = 본 doc Section 6 (commit / PR 전략) 그대로 (α 옵션 — `writing-plans` skip + design doc 이 곧 plan, 2026-05-19 사용자 결정).

## 0. 약어 풀이 (jargon 6 keyword)

- **SoT** = Source of Truth (단일 출처) — 어떤 사실 / 결정 / 규칙이 여러 곳에 정의될 수 있을 때 단일 권위 위치
- **SP** = Sub-Project (분할 작업 단위) — 본 session 안에서 분할 권고된 5 작업 단위 (SP1-SP5)
- **DE** = Data Engineering (데이터 엔지니어링)
- **ToS** = Terms of Service (이용 약관)
- **SLO** = Service Level Objective (서비스 수준 목표)
- **LLM** = Large Language Model (대형 언어 모델)
- **UGC** = User-Generated Content (사용자 생성 콘텐츠)
- **API** = Application Programming Interface
- **sd-* / kb-* / cf-*** = seoul-density / kakao-bar-nearby / cafefinder 의 차용 항목 ID

## 1. 배경 + 목적

### 1.1 배경

2026-05-14 사용자 결정 (Day 16 외부 가게 정보 도입, memory `[[project-identity-correction]]` SoT) 직후 사용자가 NomaDamas k-skill 의 [seoul-density](https://github.com/NomaDamas/k-skill/blob/main/docs/features/seoul-density.md) + [kakao-bar-nearby](https://github.com/NomaDamas/k-skill/blob/main/docs/features/kakao-bar-nearby.md) 두 skill 과 [cafefinder](https://cafefinder.virtuonweb.com/cafefinder/home) 를 reference 로 추가 제시. 본 session = 그 reference 정독 + 본 프로젝트 plan / spec / CLAUDE.md 갱신.

### 1.2 목적

- (a) 본 프로젝트와 reference 의 차이 / 차용 가능 요소 정리
- (b) Day 17 임계값 1-100 가정 오류 정정 (commit `97ef5f9` 흡수)
- (c) Day 11 본격 진입 전 4 SoT (CLAUDE.md / spec / plan / 신규 ToS doc) drift 일괄 정리
- (d) 다중 출처 정책 (카카오 평점 + 네이버 영업) 확정 + Day 16 본문에 박기

### 1.3 결과 (deliverable)

5 commit / 6 file changed / ~530-800 LOC. 신규 doc 2개 (design doc 본 file + ToS 거버넌스 doc) + CLAUDE.md / spec / plan SoT 동시 갱신.

### 1.4 기대 효과

- Day 16 / Day 17 진입 시점 가정 검증 의무 제거
- main SoT drift 0
- reference 비교 + ToS 입장 명시 → 강화 리포트 v2 (Day 18) 의 page 구성 base 확보

## 2. 정독 결과 — 두 skill + cafefinder

### 2.1 NomaDamas k-skill seoul-density

| 차원 | 본문 |
|---|---|
| API | `https://k-skill-proxy.nomadamas.org/v1/seoul-density/citydata` → 서울 열린데이터 광장 `citydata_ppltn/1/1/{area}` proxy |
| 인증 | proxy server 에 `SEOUL_OPEN_API_KEY` 보관 |
| 응답 | 원본 JSON + `proxy.cache.hit` 메타 |
| enum | "여유" / "보통" / "약간 붐빔" / "붐빔" (본 프로젝트 동일) |
| 호출 단위 | 사용자 query 시점 (단건 area) |
| 적재 | 없음 (proxy 응답 즉시) |

### 2.2 NomaDamas k-skill kakao-bar-nearby

| 차원 | 본문 |
|---|---|
| API | `https://m.map.kakao.com/actions/searchView?q=<query>` → anchor → `https://place-api.map.kakao.com/places/panel3/<confirmId>` |
| 응답 | 정규화 JSON 카드 (`openStatus.{label, detail}` / `meta.openNowCount` / `seatingKeywords` / 전화) |
| 워크플로우 | 위치 query → anchor 후보 선택 → 위치+'술집' 검색 → panel3 정규화 |
| fallback | "panel3 406 가능 (UA/Referer 필수)" / "메뉴 비면 카테고리 근사" |

### 2.3 cafefinder

| 차원 | 본문 |
|---|---|
| UX | 지도 + 필터 패널 (요일 / 오픈 / 마감 / 라스트오더) |
| 마커 색상 | 별 (24h) / 녹색 (영업) / 빨강 (영업 X) / 회색 (요일닫음) / 흰색 (정보없음) |
| 인터랙션 | "현 지도에서 재검색" + "내 위치 기준" geolocation |
| 인증 | 없음 (조회 only) |

## 3. SP1-SP5 분할 + (d) 통합 PR 채택

본 brainstorm 중 사용자가 "프로젝트 전 범위 수정" 의향 표현. brainstorming skill 가이드의 scope check (`brainstorming.md` §"if the request describes multiple independent subsystems ... flag this immediately") 정합 → 5 sub-project 분할 권고:

| SP | 본문 | 추정 | 시점 |
|---|---|---|---|
| SP1 | intent 재정의 (CLAUDE.md / spec / memory) | 1.0d | Day 11 진입 전 |
| SP2 | Day 16 데이터 흐름 구체화 | 0.5d | Day 16 진입 직전 |
| SP3 | Day 17 mart + Superset 정교화 | 0.5d | Day 17 진입 직전 |
| SP4 | Day 7-8 UX retrofit | 2.0d | Day 18 흡수 또는 Phase 2 W1 |
| SP5 | 강화 리포트 v2 페이지 구성 | 1.0d | Day 18 본격 |

사용자 결정 (2026-05-19) = **(d) 통합 plan-update PR** 채택. SP1 + SP2 + SP3 + Day 17 임계값 정정 PR 통합 = 본 PR scope. SP4 / SP5 = 본 PR scope 외.

## 4. 다중 출처 정책 — 카카오 평점 + 네이버 영업

사용자 결정 (2026-05-19) 정정 — 단일 출처 X, 다중 출처 분리:

| 필드 | 출처 | reference / endpoint |
|---|---|---|
| `rating` / `visitor_review_count` / `blog_review_count` | **카카오맵 panel3** | kakao-bar-nearby skill 검증 endpoint |
| `place_name` / `category` / `address` / `phone` | **네이버지도 backchannel** | endpoint TBD (Day 16 진입 직전 결정) |
| `open_status_label` / `open_status_detail` / `open_hours_json` | **네이버지도 backchannel** | 동일 |
| `service_options` | **네이버지도 backchannel** | 동일 |

### 4.1 `places_external` schema (검증된 SearchAPI Knowledge Graph form 차용, 18 컬럼)

```sql
CREATE TABLE places_external (
  -- identity
  biz_reg_no TEXT,
  place_id_external TEXT NOT NULL,
  source TEXT NOT NULL,                      -- 'kakao' / 'naver'
  fetched_at TIMESTAMP NOT NULL,

  -- 공통
  place_name TEXT,
  category TEXT,
  address TEXT,
  road_address TEXT,
  latitude DOUBLE,
  longitude DOUBLE,

  -- 평점 (카카오 우선)
  rating DOUBLE,
  visitor_review_count INT,
  blog_review_count INT,

  -- 연락처 (네이버 우선)
  phone TEXT,

  -- 영업 상태 / 영업시간 (네이버 우선)
  open_status_label TEXT,
  open_status_detail TEXT,
  open_status_source TEXT,
  open_status_updated_ts TIMESTAMP,
  open_hours_json TEXT,

  -- 서비스 옵션
  service_options TEXT,

  PRIMARY KEY (place_id_external, source)
);
```

### 4.2 네이버지도 정독 결과 (2026-05-19)

- 공식 API (`/v1/search/local.json`) = 영업시간 / 평점 미제공 확정
- 비공식 backchannel URL pattern = 공식 문서 부재. third-party (SearchAPI / Apify / Scrapfly) wrapping 만 존재
- **검증 가능**: schema 그릇 (필드 정의) — SearchAPI Knowledge Graph 응답 form 으로 차용
- **검증 미완**: 정확한 endpoint URL pattern — Day 16 진입 직전 결정 (DevTools inspect / third-party / 공식+Google 통합 중)

## 5. Day 16 plan-update 본문

### 5.1 Task 16.1 정정 — 다중 출처 결정 doc

`docs/decisions/2026-05-DD-external-place-source.md` 신규 (Day 16 진입 직전 작성, 본 PR scope 외):
- 다중 출처 결정 trace
- kakao 검증 endpoint (skill 차용)
- naver endpoint TBD (trigger 명시)
- attribution 분리 정책
- Phase 2 Google Places 대체안

### 5.2 Task 16.2 정정 — `places_external` + 2 fetcher

- `src/scrapers/kakao_local.py` = panel3 backchannel + 평점만 추출 + UA/Referer 헤더 + 7-30일 캐싱 + 일 300k rate limit
- `src/scrapers/naver_local.py` = endpoint TBD (placeholder) + SearchAPI Knowledge Graph form 기준 8 필드 추출
- `airflow/dags/external_places_fetch.py` = TaskGroup 2 + 야간 02-05시 실행

### 5.3 Task 16.3 정정 — `chill_open_now` mart 다중 출처 join

- 영업 = `places_external WHERE source='naver'` 우선 → 공공 인허가 fallback
- 평점 = `places_external WHERE source='kakao'` 단일
- 가게이름 / 전화번호 = `places_external WHERE source='naver'` 우선
- mart 컬럼 추가: `rating` / `rating_source` / `open_status_source` / `open_status_updated_ts`

### 5.4 fallback (kb-5 차용 + 자체)

| 상황 | 정책 |
|---|---|
| kakao 406 | UA/Referer 재시도 3회 → skip + Discord warning |
| panel3 schema drift | raw_payload 보존, 정규화 layer 재처리 |
| naver endpoint schema 변경 | Day 16 진입 직전 plan-update commit 으로 정정 |
| naver endpoint 정독 불가 | third-party 또는 공식+Google 통합 fallback |

## 6. Day 17 plan-update + 임계값 정정 + kb-2 차용

### 6.1 임계값 정정 (commit `97ef5f9` 흡수)

| `avg_congest_score` 구간 | 등급 |
|---|---|
| `<= 1.5` | `여유` |
| `(1.5, 2.0]` | `보통` |
| `(2.0, 3.0]` | `약간 붐빔` |
| `> 3.0` | `붐빔` |

정정 근거 3건 (commit `97ef5f9` 본문 그대로):
1. `src/flink_jobs/lib/transforms.py:10-15` `CONGEST_LEVEL_MAP` 실측 enum 1-4
2. `src/flink_jobs/silver_to_gold.py:132` `AVG CAST DOUBLE` → 실측 1.0-4.0
3. `dbt/seoul/models/marts/schema.yml` `chill_open_now.avg_congest_score <= 2` 정합 (여유 ∪ 보통 = chill)

### 6.2 mart `congestion_grade_5min` schema 정교화 (kb-2 차용)

신규 컬럼:
- `open_now_count` INT — 자치구별 "지금 영업 중" 가게 수 (kakao-bar-nearby `meta.openNowCount` 패턴 차용)
- `chill_open_count` INT — 자치구별 "한가 + 영업 중" 가게 수 (발화 의도 #2 직접 시각화)
- `congest_grade` TEXT — 등급 enum (`여유` / `보통` / `약간 붐빔` / `붐빔`)

dbt test:
- `accepted_values: ['여유', '보통', '약간 붐빔', '붐빔']`
- `chill_open_count <= open_now_count` (정합)

### 6.3 Superset dashboard 4 tile

| tile | 시각화 | 의미 |
|---|---|---|
| 1 | 자치구별 혼잡도 등급 heatmap | 25 자치구 × 4 등급 색상 |
| 2 | 자치구별 `open_now_count` bar | "지금 영업 중" 가게 수 |
| 3 | 자치구별 `chill_open_count` bar | "한가 + 영업 중" — 발화 의도 #2 |
| 4 | 등급별 자치구 개수 pie | 25 자치구 등급 분포 |

## 7. ToS 거버넌스 doc — `docs/governance/external_data_tos.md` 신규

### 7.1 위치 결정 (2026-05-19)

후보 3:
- (a) `docs/governance/external_data_tos.md` 신규 → **채택**
- (b) `CLAUDE.md §13` 통합 → X (가독성)
- (c) `memory/kakao-tos-position.md` → X (git tracked X)

### 7.2 본문 8 절 골격

1. 입장 (다중 출처 + risk 인지 + 채용 기대 + Phase 2 대체안)
2. 출처별 정책 (kakao endpoint + naver endpoint TBD)
3. attribution 분리 표시
4. fallback (kb-5 + 자체)
5. Phase 2 진화 path (W2 UGC / W6 Google / W8 attribution 재검토)
6. 면접 시점 답변 framework
7. memory link (`[[skill-references-integration]]` / `[[project-identity-correction]]` / `[[deferred-items-post-day10]]`)
8. 변경 trace (2026-05-14 / 2026-05-19)

### 7.3 link 매트릭스 (본 doc 으로의 link 5 위치)

CLAUDE.md §3 + §13 / spec §4 / plan Day 16 / memory `skill-references-integration` / memory `project-identity-correction`.

## 8. commit / PR 전략 (Section 6, implementation 본문)

### 8.1 branch

신규 `docs/skill-references-integration` (main 분기). 기존 `docs/day-17-threshold-correction` 의 commit `97ef5f9` 는 cherry-pick 으로 흡수.

### 8.2 commit 분할 (5 commit + design doc commit, squash merge 전제)

| # | type(scope): subject | 파일 | 추정 LOC |
|---|---|---|---|
| 0 | `docs(spec): brainstorm session design doc 신설 — k-skill references integration` (본 file) | `docs/superpowers/specs/2026-05-19-skill-references-integration-design.md` | ~350 |
| 1 | `docs(plan): Day 17 Task 17.1 임계값 정정 — 1-100 가정 → 실측 enum 1-4 (Option B)` (cherry-pick `97ef5f9`) | `phase-1b-week-3.md` | +17 -2 |
| 2 | `docs(governance): external_data_tos.md 신설 — kakao/naver ToS 입장 + 다중 출처 정책` | `docs/governance/external_data_tos.md` | ~250 |
| 3 | `docs(claude+spec): intent 재정의 — 다중 출처 + sd-3 attribution + ToS doc link` | `CLAUDE.md` + spec | ~70 |
| 4 | `docs(plan): Day 16 본문 정정 — 다중 출처 schema (kb-1) + fallback (kb-5) + naver endpoint TBD` | `phase-1b-week-3.md` | ~120 |
| 5 | `docs(plan): Day 17 schema kb-2 + Superset 4 tile + 임계값 정정 흡수` | `phase-1b-week-3.md` | ~70 |
| **합계** | | **6 file** | **~877 LOC** |

### 8.3 PR body 구조 (korean-conventions SoT)

`## 1. 개요 → 1.1-1.4 → ## 2. 의사결정 & Trade-off → ## 3. 변경 사항 → ## 4. 검증 → ## 5. 후속 작업 → ## 6. 레퍼런스`. 영어 section header 금지 + AI footer 금지 + Co-Authored-By trailer 의무.

### 8.4 self-check (korean-conventions SoT)

| 항목 | 점검 |
|---|---|
| "영역" 단어 | 0건 |
| "(portfolio 어필)" | 0건 |
| tilde-range | 0건 (hyphen 사용) |
| AI footer | 0건 |
| jargon 풀이 6 keyword | 본문 첫 등장 시 풀이 박힘 (본 §0) |
| Co-Authored-By trailer | 6 commit 모두 |

### 8.5 squash merge 정책

- Day 단위 PR 정합 (memory `[[execution-policy]]` SoT)
- doc only PR = 200 LOC 임계 적용 X (PR #66 Day 10 archive ~600 LOC doc PR 패턴 reuse)

## 9. 차용 시점 매트릭스 (sd-* / kb-* / cf-*)

### 9.1 차용 항목 표

| ID | 차용 요소 | 본 프로젝트 반영 위치 | 시점 |
|---|---|---|---|
| sd-1 | 응답 메타 (`proxy.cache.hit`) | `/api/hotspots` 응답 헤더 | Phase 2 W4 |
| sd-2 | 새벽 01-05시 미제공 fallback UX | API 응답 + frontend 회색 마커 | SP4 (Day 18 / Phase 2 W1) |
| sd-3 | **데이터 출처 attribution** | CLAUDE.md §13 + ToS doc + API 응답 | **본 PR (SP1)** |
| sd-4 | 단건 area_code route | `/api/hotspots/<area_code>` | Phase 2 W2 |
| sd-5 | 121개 핫스팟 전체 확장 | `hotspot_regions.csv` | Phase 2 W3 |
| kb-1 | `openStatus.{label, detail, source, updated_ts}` schema 그릇 | `places_external` schema | **본 PR (SP2)** |
| kb-2 | `meta.openNowCount` 집계 필드 | Day 17 mart `open_now_count` | **본 PR (SP3)** |
| kb-3 | 위치 anchor 패턴 + bbox query | `/api/hotspots?bbox=...` | Phase 2 W2-W3 |
| kb-4 | `seatingKeywords` 배열 schema | `dim_place` array type | Phase 2 W2 (UGC) |
| kb-5 | **fallback 정책 명시화** | Day 16 본문 + ToS doc | **본 PR (SP2)** |
| cf-1 | 요일×시간 필터 UX | Day 7 next.js | SP4 |
| cf-2 | 마커 색상 5단계 | Day 7 frontend | SP4 |
| cf-3 | "현 지도에서 재검색" | `/api/hotspots?bbox` | SP4 / Phase 2 W3 |
| cf-4 | geolocation API | frontend | SP4 |
| cf-5 | 언어 EN/한국어 토글 | frontend | Phase 2 W8 |

### 9.2 본 PR scope 안 차용 = 4건 (sd-3 + kb-1 + kb-2 + kb-5)

나머지 11건 = Phase 1B 잔여 (Day 18 SP4) + Phase 2 흡수.

## 10. fallback / risk

§5.4 Day 16 fallback + §7.2 ToS doc 본문 §4 fallback 정책 동일 (cross-reference).

## 11. 변경 trace

| 일자 | 변경 |
|---|---|
| 2026-05-07 | 프로젝트 정체성 정정 — 발화 의도 5건 명시화 (memory `[[project-identity-correction]]`) |
| 2026-05-14 | Day 16 외부 가게 정보 도입 결정 (카카오 / 네이버 / 스크래핑 우선순위 미정) |
| 2026-05-17 | Day 17 임계값 가정 오류 발견 (1-100 vs 실측 1-4) |
| 2026-05-18 | 임계값 정정 PR commit `97ef5f9` 작성 (단독 PR 생성 보류) |
| **2026-05-19** | **본 brainstorm 세션 — reference 정독 + 다중 출처 정책 + 통합 PR 결정** |
| Day 11 본격 진입 (~5/22) | 본 PR 머지 후 진입 |
| Day 16 진입 직전 (~5/27) | naver endpoint 본문 separate plan-update commit |
| Day 18 (~5/29) | SP4 / SP5 흡수 또는 Phase 2 이월 |

## 12. 레퍼런스

- [NomaDamas k-skill kakao-bar-nearby](https://github.com/NomaDamas/k-skill/blob/main/docs/features/kakao-bar-nearby.md) — kb-1 / kb-2 / kb-5 차용 source
- [NomaDamas k-skill seoul-density](https://github.com/NomaDamas/k-skill/blob/main/docs/features/seoul-density.md) — sd-* 차용 source
- [cafefinder](https://cafefinder.virtuonweb.com/cafefinder/home) — cf-* 차용 source
- [SearchAPI Naver docs](https://www.searchapi.io/docs/naver-api) — naver `places_external` schema 그릇 검증 source
- [Scrapfly Naver scraping guide](https://scrapfly.io/blog/posts/how-to-scrape-naver) — 공식 API 한계 확인
- [NAVER Cloud Platform Maps overview](https://api.ncloud-docs.com/docs/application-maps-overview) — 공식 API 정독
- 임계값 정정 commit (cherry-picked): `97ef5f9`
- 본 프로젝트 SoT: `CLAUDE.md` / `docs/superpowers/specs/2026-04-30-...phase1-design.md` / `docs/superpowers/plans/phase-1b-week-3.md`
- memory SoT: `[[project-identity-correction]]` / `[[airflow-decision]]` / `[[execution-policy]]` / `[[korean-conventions]]` / `[[claude-coauthor-trailer]]` / `[[deferred-items-post-day10]]`
