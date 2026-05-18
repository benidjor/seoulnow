# 외부 가게 정보 출처 거버넌스 (External Data ToS)

> Phase 1B Day 16 의 외부 가게 정보 출처 정책 단일 출처 (SoT).
>
> CLAUDE.md §3 / §13 + spec §4 + plan-1b-week-3.md Day 16 + memory `[[project-identity-correction]]` / `[[skill-references-integration]]` 모두 본 doc 으로 link.
>
> 본 doc 의 source of design = `docs/superpowers/specs/2026-05-19-skill-references-integration-design.md` §7.

## 0. 약어 풀이

- **ToS** = Terms of Service (이용 약관)
- **UGC** = User-Generated Content (사용자 생성 콘텐츠)
- **API** = Application Programming Interface
- **DE** = Data Engineering
- **SoT** = Source of Truth (단일 출처)
- **UA** = User-Agent (HTTP 헤더)
- **TTL** = Time To Live (캐시 만료 시간)

## 1. 입장

2026-05-14 사용자 결정 + 2026-05-19 reference 정독 정정 결과:

- **출처**: 카카오맵 panel3 backchannel (평점) + 네이버지도 backchannel (가게이름 / 영업시간 / 영업상태 / 전화번호) 다중 출처
- **공식 API 한계 확정** (2026-05-19 정독): 카카오 로컬 API + 네이버 검색 API 모두 평점·영업시간 미제공. 본 프로젝트 발화 의도 #2 (영업시간 접근) + #3 (cafefinder 차별화) 충족 불가.
- **risk 인지**: kakao + naver 비공식 backchannel = 명시적 ToS 위반 risk
- **채용 기대**: 본 데이터 출처가 DE 포트폴리오의 핵심 가치는 아님. DE 가치 = streaming Kafka + Flink + Iceberg + dbt + Airflow 본진 + SLO 운영 + 멱등성 + 다중 소스 통합 등 platform 차원.
- **Phase 2 대체안**: Google Places API (월 $200 무료 크레딧) 또는 자체 UGC 별점 / 자체 영업시간 입력 (P2 W2-W6 흡수)

## 2. 출처별 정책

### 2.1 카카오맵 panel3 backchannel

- **endpoint** (NomaDamas kakao-bar-nearby skill 검증, 2026-05-19 정독):
  - 검색: `https://m.map.kakao.com/actions/searchView?q=<query>`
  - anchor 후보 선택 → confirmId 추출
  - 상세: `https://place-api.map.kakao.com/places/panel3/<confirmId>`
- **추출 필드** (본 프로젝트는 다중 출처 분리 정책에 따라 `rating` 만 사용):
  - `rating` (DOUBLE)
  - `visitor_review_count` (INT)
  - `blog_review_count` (INT)
- **HTTP 헤더 필수**: User-Agent + Referer (skill 의 "406 fallback" SoT — panel3 JSON 은 브라우저 유사 헤더 없으면 406 가능)
- **rate limit**: 일 300k (카카오 개발자 콘솔 무료 한도 기준)
- **캐싱 TTL**: 7-30일
- **attribution**: "카카오맵 출처" frontend 표시 + 응답 `source_attribution.rating = "kakao"` 메타

### 2.2 네이버지도 backchannel

- **endpoint**: **TBD (Day 16 진입 직전 결정)**
  - 정독 결과 (2026-05-19): 비공식 backchannel URL pattern 공식 문서 부재 확인
  - 후보 3 path:
    1. 직접 backchannel scraping (사용자 DevTools Network 탭 inspect 결과 결정)
    2. third-party wrapping ($0-50/월): SearchAPI / Apify / Scrapfly
    3. 공식 `/v1/search/local.json` (영업시간 부재) + Google Places 통합
- **추출 필드**:
  - `place_name` (TEXT)
  - `category` (TEXT)
  - `address` (TEXT)
  - `road_address` (TEXT)
  - `phone` (TEXT)
  - `open_status_label` (TEXT — "영업 중" / "영업 종료" / "휴무" enum)
  - `open_status_detail` (TEXT — "24:00 까지" 자유 텍스트)
  - `open_hours_json` (TEXT — `[{"name": "수", "value": "11:30 - 21:30"}, ...]`)
  - `service_options` (TEXT — `["포장", "예약"]` JSON array, 선택)
- **schema form**: SearchAPI Knowledge Graph 검증 form 차용 (2026-05-19 정독)
- **HTTP 헤더 필수**: endpoint 결정 후 확정
- **rate limit**: endpoint 결정 후 확정 (third-party 사용 시 그 서비스의 한도)
- **캐싱 TTL**: 7-30일
- **attribution**: "네이버지도 출처" frontend 표시 + 응답 `source_attribution.opening = "naver"` 메타

## 3. attribution 분리 표시 정책

frontend (Day 7 next.js 페이지 / Day 17 Superset / 강화 리포트 v2) 모두 적용:

- 평점 옆 → "카카오맵 출처"
- 영업 정보 / 가게이름 / 전화번호 옆 → "네이버지도 출처"
- footer 1줄 → "외부 가게 정보 출처: 카카오맵 / 네이버지도. 상세 → `/privacy` 페이지."

API 응답 (FastAPI `/api/places/<id>`) 의 JSON body 또는 헤더에 `source_attribution` 메타:

```json
{
  "place_id": "...",
  "rating": 4.68,
  "open_status_label": "영업 중",
  ...
  "source_attribution": {
    "rating": "kakao",
    "visitor_review_count": "kakao",
    "blog_review_count": "kakao",
    "place_name": "naver",
    "phone": "naver",
    "open_status": "naver",
    "open_hours": "naver",
    "service_options": "naver"
  }
}
```

## 4. fallback 정책

NomaDamas kakao-bar-nearby skill 의 fallback 패턴 (kb-5 차용) + 본 프로젝트 자체 정책:

| 상황 | 정책 |
|---|---|
| kakao panel3 응답 406 (header 변경 시) | User-Agent / Referer 재시도 (`tenacity.retry` 3회 + `wait_exponential`). 3회 실패 시 해당 row skip + Discord webhook warning. |
| kakao panel3 schema drift (필드 추가 / 삭제) | `raw_payload` JSONB 보존, 정규화 layer 만 재처리 (`fact_user_event` pattern 과 동일) |
| 네이버지도 endpoint 응답 schema 변경 | Day 16 진입 직전 결정한 endpoint 본문 + plan-update commit 으로 정정. raw_payload 보존. |
| API 키 / rate limit 초과 | `wait_exponential` retry + 캐싱 TTL 연장 (7일 → 14일). Discord warning. |
| 네이버지도 endpoint 자체가 정독 불가 시 | (a) third-party 유료 ($0-50/월) 도입, (b) 공식 `/v1/search/local.json` + Google Places 통합 fallback (§1 의 P2 대체안 당김) |
| 메뉴 / 카테고리 비어 있을 때 (kakao) | 카카오맵 카테고리 + 소개 문구로 근사 설명 (skill SoT) — 단 본 프로젝트는 평점만 사용하므로 영향 X |
| 정확 수용 인원 / 좌석 정보 부재 | 근사 힌트로 안내 ("단체 방문 가능" 등) — Phase 2 UGC 시점에 본격화 |

## 5. Phase 2 대체 / 진화 path

| 시점 | 변경 |
|---|---|
| Phase 2 W2 | 자체 UGC 별점 도입 → 카카오 의존도 단계적 축소. `dim_place` 다출처 머지 시작. |
| Phase 2 W6 | Google Places API 도입 검토 → 카카오 + 네이버 fallback 으로 강등. 캐싱 7-30일 + 증분 갱신 정책. |
| Phase 2 W8 | 외부 공개 (OKKY / Reddit / Disquiet) 시점 attribution 정책 재검토. 실제 viral 시 카카오 / 네이버 cease-and-desist risk 명시 평가. Google Places 본격 의존 검토. |

## 6. 면접 시점 답변 framework

면접관이 "외부 데이터 출처가 ToS 위반 아닌가?" 질문 시:

- 회피 X — risk 인지 + 의도적 채택 솔직히 명시
- DE 가치 ≠ 데이터 출처. DE 가치 = streaming + 다중 소스 통합 + SLO 운영 + 멱등성 + Airflow 본진 등 platform 차원
- 외부 공개 / 실서비스 ramp 시점에는 Google Places + 자체 UGC 로 대체할 path 명시 (Phase 2 W6+)
- 본 프로젝트의 본질 = "데이터 플랫폼 + 그 위의 작은 서비스" — 데이터 출처는 서비스 측면의 한 차원일 뿐, 플랫폼 자체의 가치와 무관

## 7. memory link

- `[[skill-references-integration]]` — sd-* / kb-* / cf-* 차용 mapping
- `[[project-identity-correction]]` — 발화 의도 5건 + 정정 trace
- `[[deferred-items-post-day10]]` — Day 10 종료 후 보류 항목 (영업시간 / 평점 출처 결정)
- `[[airflow-decision]]` — 3계층 분리 원칙 + plan-update 시점

## 8. 변경 trace

| 일자 | 변경 |
|---|---|
| 2026-05-14 | 사용자 결정 — Day 16 외부 가게 정보 도입 (카카오 또는 네이버 또는 스크래핑, 우선순위 미정) |
| 2026-05-19 | brainstorm 결과 정정 — 다중 출처 (카카오 평점 + 네이버 영업) 분리 정책. NomaDamas kakao-bar-nearby skill 차용. 네이버 endpoint TBD (Day 16 진입 직전 결정). 본 doc 신설. |
| Day 16 진입 직전 (예상 2026-05-27 근처) | 네이버지도 endpoint 본문 확정 + 본 doc §2.2 갱신 |
| Phase 2 W2 (예상) | UGC 별점 도입 → 본 doc §5 진화 path 활성 |
| Phase 2 W6 (예상) | Google Places API 도입 검토 → 본 doc §5 진화 path 활성 |
| Phase 2 W8 (예상) | 외부 공개 시점 attribution 재검토 → 본 doc §5 진화 path 활성 |
