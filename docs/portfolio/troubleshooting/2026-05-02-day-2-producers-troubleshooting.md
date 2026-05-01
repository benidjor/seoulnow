# Day 2 producers — 5건 기술 트러블슈팅 + 1건 프로세스 개선

**발생일**: 2026-05-01 ~ 2026-05-02 (Phase 1A Day 2)
**관련 plan**: `docs/superpowers/plans/phase-1a-week-1.md` Task 2.1 / 2.2 / 2.3
**관련 spec**: `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` §6-1 Day 2, §6-2 (`api_response_ts` 헤더 → SLO)
**관련 PR**:
- 기술 PR — [#5](https://github.com/benidjor/seoul-citydata-platform/pull/5) (Task 2.1 config), [#7](https://github.com/benidjor/seoul-citydata-platform/pull/7) (Task 2.2 hotspot), [#8](https://github.com/benidjor/seoul-citydata-platform/pull/8) (Task 2.3 subway)
- 컨벤션 PR — [#4](https://github.com/benidjor/seoul-citydata-platform/pull/4) (template v1), [#6](https://github.com/benidjor/seoul-citydata-platform/pull/6) (v2), [#9](https://github.com/benidjor/seoul-citydata-platform/pull/9) (v3)

## 요약

Day 2 의 두 producer (hotspot + subway) 도입 + Python 프로젝트 골격 (uv / pyproject / `platform_common`) 작업 중 5건의 기술 이슈를 발견 / 해결했고, 1건의 프로세스 개선 (PR template 진화 + Task 단위 분할) 을 거쳤다. 모두 **의사결정 trade-off 가 있는 fix** 또는 **재발 가능성이 있는 운영 이슈** 로, 별도 archive 가치가 있어 정리한다.

| # | 이슈 | 카테고리 | 발견 경로 | 해결 PR |
|---|------|---------|---------|--------|
| 1 | `tenacity` / `pyiceberg` / `pyarrow` / `apache-flink` 의존성 4중 충돌 | 의존성 / lock | implementer (`uv sync`) | #5 |
| 2 | `minio_password` 평문 노출 (`repr` / `model_dump` / plan SoT 5곳 모순) | 보안 / SoT 일관성 | code review | #5 |
| 3 | URL 경로 API 키 노출 위험 (`httpx.HTTPStatusError` 의 `str(e)`) | 보안 / 구조적 | code review | #7 / #8 |
| 4 | producer flush 누락 (`run()` 의 `try/finally` 부재) | 데이터 안정성 / streaming | code review | #7 / #8 |
| 5 | subway 빈 응답 가시화 (Day 4 SLO 디버깅 사전 대응) | 운영 디버깅 | code review | #8 |
| 6 | (메타) PR template 진화 + Task 단위 분할 | 프로세스 개선 | 사용자 직접 지적 | #4 / #6 / #9 + 분할 4 PR |

기술 이슈 5건의 진단 + fix 합산 약 80분 (각 평균 16분). 메타 1건 (PR template 진화 + 분할) 약 3시간. 모두 plan 본문 §9 의 fallback 트리거 (의존성 / 보안 / 운영) 발동 없이 정상 경로 closure.

---

## 1. `tenacity` / `pyiceberg` / `pyarrow` / `apache-flink` 의존성 4중 충돌

### 1.1. 증상

`uv sync --extra dev` 실행 시 의존성 resolve 실패. plan 본문의 `tenacity>=9.0` + `pyiceberg>=0.7` + `apache-flink==1.20` (extra) 조합이 동시 만족 불가능.

### 1.2. 원인 — 의존성 chain 분석

연쇄 제약:

- `pyiceberg 0.7.x` 는 `tenacity<9.0` 요구
- `flink` extra 의 `apache-flink 1.20` 이 transitively `apache-beam<2.49` 끌어옴
- `apache-beam<2.49` 는 `pyarrow<12` 요구
- 반면 `pyiceberg≥0.8` 은 `pyarrow≥14` 요구

두 제약 (`pyarrow<12` vs `pyarrow≥14`) 동시 만족 불가능 → uv 가 `pyiceberg` 를 0.7.x 로 고정 → `tenacity<9` 도 강제됨.

### 1.3. 해결

- `pyproject.toml` — `tenacity>=9.0` → `tenacity>=8.2`
- 코드 영향 0 — retry API 가 `tenacity 8.x` 와 `9.x` 동일
- plan 본문도 동일하게 동기화 (Task 2.1 코드 블록 + "버전 메모" blockquote 한 줄 추가)

### 1.4. 검증

```bash
$ uv sync --extra dev
Resolved 91 packages in 16ms
Checked 61 packages in 9ms

$ uv tree | grep -E "tenacity|pyiceberg"
pyiceberg[duckdb, s3fs] v0.7.1
└── tenacity v8.5.0
```

`uv.lock` 메타데이터 직접 inspect — pyiceberg 0.7.1 의 tenacity 의존이 `<9.0` 임을 사실 확인.

### 1.5. 교훈

- "최신 lower bound 가 항상 옳지 않다" — extra 의 transitive 가 메인 의존성 (pyiceberg) 의 버전을 강제할 수 있음
- `pyproject` 의 lower bound 는 *진짜로 필요한 최저 버전* 만 명시 — `uv` 가 자동 계산하도록 두는 게 안전
- Phase 2 에서 `apache-flink` 가 `pyarrow 14+` 를 지원하기 시작하면 `pyiceberg 0.8+` 로 업그레이드 + `tenacity` 도 9.x 로 다시 올리기 검토

---

## 2. `minio_password` 평문 노출 (보안 + SoT 일관성)

### 2.1. 증상

PR-B (Task 2.1) code review 1차 라운드에서 `minio_password: str` 의 평문 노출 위험 지적. 실측:

- `repr(settings)` 출력에 `minio_password='minioadmin'` 평문
- `model_dump()` 결과에서도 평문

### 2.2. 원인

pydantic v2 의 default 동작 — `str` 타입 필드는 `repr` / `dump` 시 그대로 출력. 후속 producer / Flink job / Airflow task 에서 `Settings` 객체를 로그로 출력하면 자격증명이 그대로 흘러나감.

추가 발견 (`SecretStr` 도입 후):

- plan 본문의 다른 코드 블록 5곳이 `SecretStr` 도입 전 패턴인 `'{s.minio_password}'` f-string 사용
  - Iceberg catalog 등록 SQL 1건
  - DuckDB SECRET 4건 (week-1 / week-2 plan 합산)
- 이대로 두면 후속 implementer 가 plan 코드를 그대로 복사할 때 `SecretStr` 의 `__str__` (`'**********'`) 이 SQL 에 박혀 인증 실패
- plan SoT 자체가 내부 모순

### 2.3. 해결

코드:

- `minio_password: str` → `minio_password: SecretStr`
- `default=SecretStr("minioadmin")` 로 명시

plan 본문 동기화:

- `pydantic` import 에 `SecretStr` 추가
- `schemas` / `kafka.py` 의 import 같이 동기화
- 5곳의 f-string 모두 `'{s.minio_password.get_secret_value()}'` 로 통일

### 2.4. 검증

```bash
$ uv run python -c "from platform_common import get_settings; \
    s = get_settings(); print(repr(s.minio_password))"
SecretStr('**********')

$ uv run python -c "from platform_common import get_settings; \
    s = get_settings(); print(s.minio_password.get_secret_value())"
minioadmin
```

`model_dump()` 안의 `minio_password` 도 `SecretStr('**********')` 마스킹.

### 2.5. 다른 secret 후보

- `seoul_openapi_key` / `subway_api_key` 는 빈 default (`""`) 의 fail-fast 패턴
- production 경로에 직접 노출되는 빈도 낮음 → 본 PR 에서는 `str` 유지
- 일관성 정리는 후속 task 에서 검토 (전부 `SecretStr` 통일 vs 일부만)

### 2.6. 교훈

- pydantic v2 의 `SecretStr` 은 마스킹 default — secret 후보 필드는 *기본적으로* `SecretStr` 우선
- `SecretStr` 도입은 "타입 변경 + 사용처 변경" 이 함께 — plan / docs / 후속 코드 모두 동기화 필수
- SoT (plan) 와 코드 일관성은 code review 가 짚을 가능성 — review 에서 발견되면 *plan 까지 동기화* 가 정석

---

## 3. URL 경로 API 키 노출 위험 (보안 / 구조적)

### 3.1. 증상

PR-C (Task 2.2 hotspot) code review 1차 라운드:

- `log.warning("fetch_failed", error=str(e))` 의 보안 위험 지적
- 서울 OpenAPI 가 인증 키를 URL 경로에 평문으로 받음 — `f"{SEOUL_API_BASE}/{api_key}/json/citydata/..."`
- `httpx.HTTPStatusError` 의 `str(e)` 출력에 URL 이 포함됨 → 키가 그대로 로그에 박힐 위험

### 3.2. 원인

`httpx` 의 `HTTPStatusError` 메시지 형식:

```
Client error '401 Unauthorized' for url 'http://openapi.seoul.go.kr:8088/ACTUAL_KEY/json/citydata/...'
```

`str(e)` 가 URL 통째 출력 → 키 노출.

현재는 tenacity `RetryError` 가 우연히 래핑해 차단했지만 구조적 의존이 취약:

- 향후 `reraise=True` 변경 시 재발
- tenacity 우회 경로 (다른 producer / Phase 2) 에서 재발

### 3.3. 해결

`except` 분기 두 개로 분리:

```python
except httpx.HTTPStatusError as e:
    # API 키가 URL 경로에 평문으로 박히므로 str(e) / URL 노출 금지
    log.warning("fetch_failed_http", area=name, status=e.response.status_code)
    continue
except Exception as e:
    log.warning("fetch_failed", area=name, error=type(e).__name__)
    continue
```

`str(e)` 사용 0 — URL 노출 경로 구조적 차단.

### 3.4. PR-D 일관 적용

- subway_producer 의 `run()` 도 hotspot 과 동일 구조 적용
- 두 producer (hotspot / subway) isomorphism 유지 — 향후 Phase 2 의 abstract base 추출 시 인터페이스 비용 0
- plan 본문도 새 패턴으로 동기화

### 3.5. 검증

- `ruff check src/` → All checks passed
- code review 재라운드 — Approved (Critical 0 / Important 0)

### 3.6. 교훈

- 인증 키를 URL 경로에 평문으로 받는 API (서울 OpenAPI 류) 는 `HTTPStatusError` 의 `str(e)` 가 *즉시 키 노출 경로*
- `str(e)` 대신 `status_code` / `type(e).__name__` 만 노출 — 구조적 차단
- 우연히 안전한 동작 (tenacity `RetryError` 래핑) 에 의존하지 말 것 — 명시적 차단 필수

---

## 4. producer flush 누락 (데이터 안정성 / streaming)

### 4.1. 증상

PR-C code review 1차 라운드:

- `run()` 의 cycle 정상 완료 시에만 `producer.flush(timeout=10)` 호출
- 예외 / 비신호 종료 시 in-flight 메시지 손실 가능
- `enable.idempotence=True` / `acks=all` 의 *exactly-once* 의도와 일관성 결여

### 4.2. 원인

기존 구조 (plan 원안):

```python
with httpx.Client() as client:
    while not stop["flag"]:
        for code, name in area_codes.items():
            ...
            produce_json(...)
        producer.flush(timeout=10)  # cycle 끝에서만
```

cycle 안에서 예외 발생 시 (다른 종류의 KeyboardInterrupt / 예상 외 RuntimeError 등) flush 안 호출되고 in-flight 메시지가 broker 도달 전에 끊어짐.

### 4.3. 해결

`try / finally` 로 producer 보호:

```python
producer = build_producer(client_id=...)
try:
    with httpx.Client() as client:
        while not stop["flag"]:
            ...
            # 매 cycle 즉시 broker commit. finally 의 flush 는 예외 / 비정상 경로 방어용
            producer.flush(timeout=10)
            ...
finally:
    # 예외 / 정상 종료 모두 flush 보장. confluent_kafka.Producer 는 close() 없음 — flush 로 충분
    producer.flush(timeout=10)
```

cycle flush + finally flush 가 둘 다 호출되는 구조:

- 정상 경로 — cycle flush 가 commit, finally 는 no-op
- 예외 경로 — cycle flush 안 호출돼도 finally 가 in-flight 메시지 보장

`confluent_kafka.Producer` 의 `close()` 메서드는 없음 — `flush` 만으로 충분.

### 4.4. 의도 주석 (PR-D 작업 중 발견된 가독성 개선)

PR-D 작업 중 cycle flush + finally flush 둘 다 있는 구조가 "중복 호출 = 버그?" 처럼 보일 수 있다는 reviewer 의견 (Minor). 한 줄 주석 추가:

```python
# 매 cycle 즉시 broker commit. finally 의 flush 는 예외 / 비정상 경로 방어용
```

PR-C 의 hotspot_producer.py 도 commit `21b3045` 로 같은 주석 추가해 isomorphism 회복.

### 4.5. 검증

- pytest 4 PASS 회귀
- `run()` 함수의 try/finally 들여쓰기 + 콜론 syntax 정상
- `SystemExit` 시 (`SEOUL_OPENAPI_KEY` 미설정) producer 정의 전이라 finally 도 no-op — 안전

### 4.6. 교훈

- `enable.idempotence=True` / `acks=all` 같은 exactly-once 설정은 *코드 흐름에서도 보장* 돼야 — 즉 finally flush 필수
- `confluent_kafka.Producer` 는 `close()` 메서드 없음 — `flush` 가 close 의 역할
- "코드의 의도가 코드에서 보여야" — 중복 호출 같은 패턴은 주석으로 명시 (오해 방지)

---

## 5. subway 빈 응답 가시화 (운영 디버깅)

### 5.1. 증상

PR-D (Task 2.3 subway) code review 1차 라운드:

- `parse_subway_payload` 가 빈 list 반환 시 `produced_batch (count=0)` 만 찍혀
- 빈 list 의 세 가지 원인을 구분 못함
  - `errorMessage.code` 오류 응답
  - `responseTime` 파싱 실패 (전체 케이스가 ValueError 로 skip)
  - 빈 `CongestionInfo` 배열

### 5.2. 원인 / 영향

Day 4 SLO 측정 시 "왜 0건?" 의 원인 추적이 매우 어려워질 위험:

- `count=0` 만 보고는 실 API 가 빈 응답을 줬는지 / 우리 파싱이 fail 했는지 / `errorMessage` 오류인지 알 수 없음
- 디버깅이 fixture 반복 실행 + manual 추적으로 떨어짐

### 5.3. 해결

```python
events = parse_subway_payload(payload)
if not events:
    # errorMessage 코드 / responseTime 파싱 실패 / 빈 CongestionInfo 등
    log.warning("parse_returned_empty", line=line)
    continue
for event in events:
    produce_json(...)
log.info("produced_batch", line=line, count=len(events))
```

빈 경우엔 `parse_returned_empty` warning 만 노출 — `produced_batch (count=0)` 자체 차단.

### 5.4. 추가 fix (Minor)

같은 review 라운드에서 두 Minor 도 함께 처리:

- `responseTime` 파싱 실패 시 무음 skip → `log.debug("skip_bad_response_time", raw=ts_raw)` 추가
  - debug level — default 안 출력, 명시적 활성화 시만 노출
  - `raw` 값이 시각 문자열이라 PII 아님
- `_on_signal` 에 `log.info` 누락 (hotspot 과 비대칭) → `log.info("shutdown signal received")` 추가

### 5.5. 검증

- pytest 8 PASS (hotspot 4 + subway 4 회귀)
- 빈 fixture 가 없는 한 실 테스트 영향 0 (`parse_returned_empty` 발화 X)

### 5.6. 교훈

- streaming producer 의 `produced_batch (count=0)` 같은 *모호한 정상 로그* 는 SLO 디버깅의 적
- 빈 응답 / 파싱 실패 / API 오류 세 케이스는 *각각 다른 로그* 로 노출해야 운영 시 즉시 식별 가능
- alert fatigue 위험 — warning 빈도 점검 필수 (60초 cycle × 2 노선 = 분당 2회 호출, 정상 시 빈 응답 거의 없음 → 안전)

---

## 6. (메타) PR template 진화 + Task 단위 분할 (프로세스 개선)

### 6.1. 발견 경로

Day 2 작업 흐름:

1. Task 2.1 / 2.2 / 2.3 모두 한 branch (`phase-1a/day-2-producers`) 에서 진행 → 모놀리식 PR #3 생성 (3092 LOC, 20 파일)
2. PR #3 description 작성 (옛 11 섹션 template 적용) 후 사용자 review
3. 사용자 직접 가독성 문제 지적

문제점:

- PR 본문이 121 라인 — 한 글로 길음
- `## 요약` / `## 배경` / `## 기대효과` 등 11 섹션 — 작성자도 reviewer 도 어디에 무엇을 적을지 모호 (요약 ↔ 배경 ↔ 기대효과 의미 겹침)
- 의사결정 narrative 가 한 `###` 헤더 안에 bullet 으로 평면 나열 — 깊이 인식 약함
- 700+ LOC 코드 — Glassdoor [optimal-pull-request-size](https://smallbusinessprogramming.com/optimal-pull-request-size/) 글의 200 LOC 권장 대비 3.5배

### 6.2. 의사결정 흐름 (3 라운드 진화)

#### 6.2.1. v1 — PR template 1차 갱신 (PR #4)

- [dbt Labs 의 PR template](https://docs.getdbt.com/blog/analytics-pull-request-template) 참고
- 옛 11 섹션 → 새 7 섹션
  - 배경 / 목적
  - 의사결정 / Trade-off
  - 변경 사항
  - 검증
  - 장애 시나리오 / 롤백
  - 체크리스트
  - 참고
- DE 관점 통합
  - 의사결정 / Trade-off — 면접 자료성 (대안 검토 / 트레이드오프 / 채택 사유)
  - 장애 시나리오 / 롤백 — 운영 감각 (데이터 손실 / 멱등성 / SLO / 보안 / 계층 의존)

#### 6.2.2. v2 — 어휘 / 가독성 / 크기 (PR #6)

- 어휘 — `/` → `&` (배경 & 목적 등), "머지 후 가치" → "기대 효과"
- sub-헤더 어휘 통일 — 충돌 발생 과정 / 발견 과정 / 코드에 미치는 영향 / 처리 방법 / 다른 후보 처리
- 헤더 깊이 가이드 — `####`, `#####` 까지 사용 권장
- 줄바꿈 / bullet 가이드 — 긴 문장에 `—` 또는 `→` 가 여러 번 등장하면 쪼개기
- 크기 기준 — 400 / 1000+ → **200 / 500+** (Glassdoor 글 기준)

#### 6.2.3. v3 — 헤더 번호 + 기대 효과 sub-헤더 (PR #9)

- `## 1.` / `### 1.1.` / `#### 1.1.1.` 점 구분 번호 매기기
- 깊이 직관적 (점 개수 = 깊이 - 1), 산업 표준 (legal / academic / spec docs)
- `## 1. 배경 & 목적` 안에 `### 1.1. 배경` + `### 1.2. 기대 효과` sub-헤더 분리
- 옛 형태 — 마지막 bullet 에 묻혔던 "기대 효과" 가시성 회복

### 6.3. PR 분할 (Task 단위 4 PR)

원본 PR #3 (모놀리식) close + Task 단위 분할:

- **PR #4** — PR template v1 + Co-Authored-By trailer 정책 (docs only)
- **PR #5** — Task 2.1 (`pyproject` + `platform_common`, +95 LOC + `uv.lock` 자동)
- **PR #7** — Task 2.2 (hotspot producer, +280 LOC, 200~500 중간)
- **PR #8** — Task 2.3 (subway producer, +220 LOC, 200~500 중간)

추가 docs PR:

- **PR #6** — 어휘 / 가독성 / 크기 v2
- **PR #9** — 헤더 번호 + 기대 효과 sub-헤더 v3

순차 머지 (옵션 X) — 한 PR 머지 후 다음 PR base 가 갱신된 main. 각 PR 의 base 가 항상 최신 main 이라 cherry-pick conflict 위험 0.

### 6.4. 시점 cutoff 원칙

PR #1 (Day 1 인프라) / PR #2 (Airflow 결정 docs) 는 옛 11 섹션 template 그대로 보존:

- retroactive 갱신 X — 컨벤션 진화 자체가 portfolio 의 자료
- 단 PR #1 / #2 본문 끝에 한 줄 footnote 로 새 template 채택 시점 cross-link 추가
- "처음엔 큰 PR + 옛 template, 이후 단계적으로 가독성 개선" 흐름 자체가 메타-개선 사고력 어필

### 6.5. 적용 결과

- Day 2 작업 종료 시점에 main 의 모든 머지된 PR (#4 ~ #9) 가 새 template 적용
- 후속 Day 3 ~ Phase 1B Day 14 + Phase 2 모두 같은 template 사용 예정
- PR description 자체가 "어떤 의사결정을 했는가" 의 자료 — 면접 / 회고 / 블로그 발췌 모두 단일 source

### 6.6. 교훈

- PR template / 컨벤션은 *고정된 것이 아니라 진화하는 것* — 첫 적용 후 가독성 / 어휘 문제는 자연 발견
- 시점 cutoff 원칙 — 머지된 PR 의 retroactive 갱신은 회피 (정보 손실 + 시간 비용 + git history 복잡)
- 200 LOC 권장 (Glassdoor 글) 은 1인 프로젝트에도 적용 가치 — 결함 검출률 + atomicity + 면접 자료성 셋 다 향상
- Task 단위 PR 분할 = "PR 1개 = 1 클래스 + 단위 테스트" (dbt 권장) 와 일치

---

## 적용된 fallback / 미적용

- plan 본문 §9 의 fallback 트리거 (의존성 / 보안 / 운영) 모두 발동 안 함
- 5 기술 이슈 합산 진단 + fix ~80분 (각 평균 16분), 메타 1건 (PR template 진화 + 분할) ~3시간
- 정상 경로로 closure

## 향후 운영 시 주의

| 상황 | 조치 |
|---|---|
| `uv sync` 가 의존성 resolve fail | • 의존성 chain 분석<br>• `extra` (flink 등) 의 transitive 까지 점검 |
| `Settings` / 자격증명 출력 시 평문 노출 의심 | • `repr(settings)` / `model_dump()` 실측<br>• `SecretStr` 미사용 여부 확인 |
| 인증 키가 URL 경로에 평문 | except 분기 분리 — `HTTPStatusError` → `status_code` / `Exception` → `type(e).__name__` |
| streaming producer 의 in-flight 메시지 손실 의심 | `run()` 의 `try/finally` + `producer.flush()` 보장 |
| `produced_batch (count=0)` 모호 | 빈 응답 / 파싱 실패 / API 오류 세 케이스를 각각 다른 log 로 분리 |
| PR description / 분량이 가독성 떨어지면 | template 진화 + Task 단위 분할 검토 (200 LOC 권장) |

## 블로그 업로드 시 발췌 가이드

각 이슈 (1~5) 가 독립적인 글 1편으로 변환 가능:

- 제목 — "ㅇㅇ 트러블슈팅 — Day 2 producers" 형식
- 구조 — 증상 / 원인 / 해결 / 검증 / 교훈
- 코드 블록 + 다이어그램 (의존성 chain 등) 추가 시 풍부

메타 트러블슈팅 (#6) 은 별도 글 — "PR template 을 단계적으로 개선한 이유" 또는 "1인 프로젝트의 PR 분할 전략" 형태로 메타-개선 사고력 어필.

## 레퍼런스

- 이전 troubleshooting docs — [`2026-05-01-lakekeeper-v05-setup.md`](./2026-05-01-lakekeeper-v05-setup.md)
- Plan — `docs/superpowers/plans/phase-1a-week-1.md` Task 2.1 / 2.2 / 2.3
- Spec — `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` §6-1 Day 2, §6-2
- Convention SoT — [`CONTRIBUTING.md`](../../../CONTRIBUTING.md), [`.github/PULL_REQUEST_TEMPLATE.md`](../../../.github/PULL_REQUEST_TEMPLATE.md)
- 외부 참고 — [dbt PR template](https://docs.getdbt.com/blog/analytics-pull-request-template), [optimal-pull-request-size](https://smallbusinessprogramming.com/optimal-pull-request-size/)
