# Day 8 — chill-open mart + streaming long-running + hygiene 학습 정착 (PR γ-1)

> 작성: 2026-05-12 01:00 KST
> 시점: Day 8 PR α (#45 chill-open mart) + hotfix (#46 streaming long-running) + PR β (#47 /chill 페이지) + hygiene 2건 (#48 backend + #49 frontend) 머지 후의 학습 자산 명문화.
> 관련 PR: 본 PR γ (docs only)
> 후속 archive: [`2026-05-12-day-8-cloudflare-deploy.md`](2026-05-12-day-8-cloudflare-deploy.md) (Cloudflare Pages + Tunnel, 스크린샷 12장 포함)

## 0. 진입 흐름 요약

Day 7 종료 (PR #41-44 + 운영 후속 archive) 후 Day 8 entry plan §5 의 결정 5건 (옵션 A 권장 채택) 따라 진행:

| PR | 항목 | 머지 |
|---|---|---|
| #45 | Day 8 PR α — chill-open mart + `/api/chill-open` (Task 8.1 정적 인허가 적재 + Task 8.2 dbt mart + API route) | ✅ |
| #46 | hotfix — `lib/lifecycle.py` 추출 (`time.sleep(SMOKE_RUN_SECONDS)` → `wait_for_shutdown()` long-running default) | ✅ |
| #47 | Day 8 PR β — Next.js `/chill` 페이지 + ChillList component (Task 8.3) | ✅ |
| #48 | hygiene (backend) — `src/api/` baseline ruff format + smoke runbook 갱신 | ✅ |
| #49 | hygiene (frontend) — `web/.eslintrc.json` 정착 | ✅ |

총 9 file 신규 + 12 file modify, 누적 ~550 사람 LOC.

## 1. PR α (#45) — dbt-duckdb python model `to_view` 패턴 (신규 학습 자산)

### 1.1 발견 시점

Task 8.2 implementer dispatch 시점. plan 본문 (line 1716-1773) 의 dbt SQL model 형식:

```sql
{{ config(materialized='view', schema='gold') }}

with district_score as (
    select ... from iceberg_scan('s3://seoul-warehouse/warehouse/gold/fact_hotspot_congestion_5min')
    ...
),
places_combined as (
    select ... from {{ ref('dim_place') }}
    union all
    select ... from read_parquet('s3://seoul-warehouse/warehouse/bronze/places_static_v1/data.parquet')
)
select ... from ranked p join district_score d using (district) ...
```

implementer 가 sanity 단계에서 두 falsify 사례 발견:

1. **`iceberg_scan('s3://...')` 의 Lakekeeper UUID-prefix path 미해결** — Day 4 archive `2026-05-09-day-4-tasks-4_1-4_3.md` §plan deviation 4 SoT
2. **`dbt.ref('dim_place')` 의 Relation 객체 f-string interpolation 시 데이터 dump** — dbt-duckdb python model 의 내부 동작, 본 시점에 첫 발견

### 1.2 사후 우회

#### Deviation 8.2-A (사전 채택) — `iceberg_scan` → `table_paths` 우회

Day 4/5/6/7 archive SoT reuse — Lakekeeper REST 가 vend 하는 UUID-prefix path을 `flink_jobs.lib.duckdb_iceberg.table_paths()` 가 catalog 의 metadata 에서 resolve. dispatch prompt 에 사전 채택 명시.

#### Deviation 8.2-B (사후 우회) — dbt SQL model → Python model + `to_view` 패턴

implementer 가 dbt SQL model 의 `{{ ref('dim_place') }}` jinja 처리 + `iceberg_scan` 직접 호출 모두 falsify 후 Python model 로 변환:

```python
# dbt/seoul/models/marts/chill_open_now.py
def model(dbt, session):
    dbt.config(materialized="view", schema="gold")

    con = session
    configure_duckdb(con)

    # 1) dbt.ref('dim_place') → Relation 객체. f-string에 직접 박으면 데이터 dump.
    #    to_view으로 view 등록 후 SQL 안에서 view name 으로 참조.
    dim_place_relation = dbt.ref("dim_place")
    dim_place_relation.to_view("dim_place_view", replace=True)

    # 2) Lakekeeper UUID-prefix path은 lib `table_paths()` 우회
    catalog = build_catalog()
    gold_paths = table_paths(catalog, "gold.fact_hotspot_congestion_5min")

    # 3) bronze 정적 parquet 는 catalog 미경유, read_parquet 직접
    s = get_settings()
    places_static_path = f"s3://{s.iceberg_warehouse_bucket}/warehouse/bronze/places_static_v1/data.parquet"

    sql = f"""
        WITH district_score AS (
            SELECT district, avg_congest_score
            FROM (
                SELECT *, row_number() OVER (PARTITION BY district ORDER BY window_start DESC) AS rn
                FROM read_parquet({gold_paths!r}, hive_partitioning = true)
            ) WHERE rn = 1
        ),
        places_combined AS (
            SELECT place_id, biz_reg_no, name, category, district, gu_code,
                   latitude, longitude, open_hour, close_hour, status
            FROM dim_place_view
            UNION ALL
            SELECT NULL AS place_id, biz_reg_no, name, category, district, gu_code,
                   latitude, longitude, open_hour, close_hour, status
            FROM read_parquet('{places_static_path}')
        ),
        ...
    """
    return con.sql(sql)
```

### 1.3 학습 가치 — 기존 dbt python model 비교

본 시점까지 dbt python model의 consumer:

| consumer | PR | 항목 |
|---|---|---|
| `stg_hotspot_silver.py` (Day 5) | PR #29 | bronze → silver staging, 다른 dbt ref X |
| `dim_place.py` (Day 6) | PR #38 | CDC silver → gold dim, 다른 dbt ref X |
| **`chill_open_now.py` (Day 8)** | PR #45 | gold mart, **`dbt.ref('dim_place')` cross-ref 사례 첫 사례** |

→ 기존은 다른 dbt model 의 ref 없음 → `to_view` 패턴 부재. 본 시점에 첫 cross-ref 사례 → **신규 학습 자산** 정착.

### 1.4 Phase 1B / 2 reuse

Phase 1B (`user.events.v1` 의 anonymous 사용자 행동 mart) / Phase 2 (UGC 별점 + Google Places merge mart)의 dbt mart 가 다른 dbt python model 의 ref 사용 시 본 패턴 reuse 의무:

```python
relation = dbt.ref("other_model")
relation.to_view("other_model_view", replace=True)
# SQL 안에서 view name 으로 참조
```

## 2. PR α (#45) — `lib/duckdb_iceberg` 의 6·7·8번째 consumer 정착

### 2.1 정착

PR #28 (Day 5 진입 전 lib 추출) 시점부터 정착된 lib `flink_jobs.lib.duckdb_iceberg.{build_catalog, configure_duckdb, table_paths}` 의 consumer:

| # | consumer | PR | 항목 |
|---|---|---|---|
| 1 | `stg_hotspot_silver.py` | #29 (Day 5) | dbt python model |
| 2 | `dim_place.py` | #38 (Day 6) | dbt python model |
| 3 | `scripts/duckdb_check.py` | #28 (Day 5 진입 전) | 운영 점검 |
| 4 | `flink_jobs/slo_metrics.py` | #28 (Day 5 진입 전) | SLO 측정 |
| 5 | `src/api/deps.py` (FastAPI) | #41 (Day 7 PR α) | API serving |
| **6** | **`scripts/load_static_places.py`** | **#45 (Day 8 PR α)** | **정적 인허가 1회 적재** |
| **7** | **`src/api/routes/chill_open.py`** | **#45 (Day 8 PR α)** | **API serving (`/api/chill-open`)** |
| **8** | **`dbt/seoul/models/marts/chill_open_now.py`** | **#45 (Day 8 PR α)** | **dbt python model (cross-ref + table_paths + configure)** |

→ 본 PR α 에서 **3 consumer 동시 정착**. lib 의 SQL injection 표면 일원화 효과 확장.

### 2.2 학습 — lib reuse의 가속

Day 7 PR γ §10-4 SoT — "lib 추출 후 매 PR 마다 consumer 추가" 패턴 정착. 본 PR α 시점의 8번째 consumer 정착 = drift 위험 0 + 의존성 명확.

Phase 1B / 2의 신규 streaming / API / mart 추가 시 같은 lib 위임 의무.

## 3. hotfix (#46) — `lib/lifecycle.py` 추출 + long-running default

### 3.1 발견 시점

Day 7 PR δ archive `2026-05-11-day-7-streaming-smoke-timeout-restart.md` §1 SoT — `bronze_to_silver.py:195` + `silver_to_gold.py:140` + `cdc_to_dim_place.py:149` 의 `time.sleep(SMOKE_RUN_SECONDS)` 패턴이 silent timeout exit 의 root cause. 본 시점에 design boundary closure 진행.

### 3.2 lib 추출

3 streaming job 의 동일 패턴 → DRY 위해 `src/flink_jobs/lib/lifecycle.py` 신규:

```python
"""Streaming job lifecycle helpers — long-running 운영 + smoke 검증 양립."""
from __future__ import annotations

import logging
import os
import signal
import threading
from types import FrameType

log = logging.getLogger(__name__)


def wait_for_shutdown() -> None:
    """SMOKE_RUN_SECONDS 환경변수에 따라 streaming main 을 대기시킨다.

    - 미설정 또는 0 (default) → long-running 모드. SIGTERM/SIGINT 까지 대기,
      1h heartbeat 로 alive 가시성 확보 (PR δ §6-1 silent exit fingerprint 회피).
    - >0 → smoke mode. N초 후 자연 종료, 단 SIGTERM 도 graceful 처리.
    """
    smoke_seconds = int(os.environ.get("FLINK_SMOKE_RUN_SECONDS", "0"))
    shutdown = threading.Event()

    def _handle_signal(_signum: int, _frame: FrameType | None) -> None:
        log.info("Received SIGTERM/SIGINT, shutting down.")
        shutdown.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    if smoke_seconds > 0:
        log.info("Streaming 가동 중 (smoke mode, %ds). SIGTERM 대기.", smoke_seconds)
        if shutdown.wait(timeout=smoke_seconds):
            log.info("SIGTERM 수신, 종료.")
        else:
            log.info("Smoke run timeout (%ds), 종료.", smoke_seconds)
        return

    log.info("Streaming 가동 중 (long-running mode). SIGTERM 대기, 1h heartbeat.")
    while not shutdown.is_set():
        if shutdown.wait(timeout=3600):
            break
        log.info("alive (1h heartbeat).")
    log.info("SIGTERM 수신, 종료.")
```

3 streaming job 모두 위임:

```python
from flink_jobs.lib.lifecycle import wait_for_shutdown

# ... t_env.execute_sql(insert_sql) 직후
wait_for_shutdown()
```

### 3.3 환경변수 default 변경 + 호환성

| | 이전 | 신규 |
|---|---|---|
| `FLINK_SMOKE_RUN_SECONDS` default | `"600"` (10분 자연 종료) | `"0"` (long-running) |
| 미설정 동작 | smoke 모드 (PR δ §1 의 silent timeout exit root cause) | **long-running 모드** (SIGTERM 까지 대기 + 1h heartbeat) |
| smoke 검증 | default 의존 | **명시적 `FLINK_SMOKE_RUN_SECONDS=600` export 의무** |
| 기존 `=90000` 우회 | 25h smoke | (동일, smoke mode 진입) |

호환성 변경 — Day 1-7 의 smoke 검증 절차 (default 600 의존) 호환성 깨짐 → hygiene PR #48 의 runbook 갱신 (§5 참조).

### 3.4 새 streaming job 추가 시 reuse 의무

Phase 1B (`user_events` consumer) / Phase 2 (버스 위치 / UGC streaming)의 신규 streaming job 추가 시 본 `wait_for_shutdown()` 위임 의무. silent exit fingerprint 회피 정공.

### 3.5 학습 — design boundary 의 명시적 closure

PR δ §6-3 SoT — 코드 주석 / docstring 의 "검토" / "임시" / "smoke" / "TODO" 표현은 future 자산. 본 hotfix 가 그 boundary 의 closure 정공 — bronze_to_silver.py:196 의 "운영 시점에는 별도 deploy mode (per-job cluster) 로 변경 검토" 폐기 + lib 위임 정착.

## 4. PR β (#47) — Item type 사후 우회 3건 (plan 본문 vs 실 응답 schema)

### 4.1 발견 시점

Task 8.3 implementer 의 sanity 단계 — Plan 본문 (line 1944-1954) 의 `ChillList.tsx` Item type:

```typescript
// plan 원안
type Item = {
  biz_reg_no: string;        // ← plan 가정
  name: string;
  category: string;
  district: string;
  open_hour: number;
  close_hour: number;
  avg_congest_score: number;
  is_open_now: boolean;
};
```

`curl /api/chill-open | python3 -m json.tool` 으로 실 응답 schema cross-check:

```json
{
  "biz_reg_no": 2208612001,       // ← 실 응답은 number
  "name": "강남 24시 김밥",
  "category": "음식점",
  "district": "강남구",
  "latitude": 37.4979,             // ← plan type 누락
  "longitude": 127.0271,           // ← plan type 누락
  "open_hour": 0,
  "close_hour": 24,
  "avg_congest_score": 2.0,
  "is_open_now": true
}
```

### 4.2 사후 우회 3건

| # | 항목 | Plan 원안 | 적용 | 사유 |
|---|---|---|---|---|
| 1 | `biz_reg_no` 타입 | `string` | **`number`** | PR #45 의 `_row_to_dict`이 datetime 만 isoformat 변환, 나머지 raw 타입 보존. 실 응답 = Integer (`2208612001`) |
| 2 | `latitude` / `longitude` | (누락) | **`number` 추가** | Plan type 정의가 PR #45 `chill_open.py` line 102-111 의 cols list 와 불일치. 실 응답 schema 10 keys 와 정합 보정 |
| 3 | `loaded` state | (미명시) | **`useState<boolean>(false)` + finally setLoaded(true)** | 초기 mount race (fetch 완료 전 빈 items 가 "조건 충족 가게 없음" 으로 잘못 표시) 회피. render cycle 정규화 |

### 4.3 PR γ §10-2 의 사후 우회 패턴 SoT 적용

Day 7 PR γ §10-2 의 판단 기준:

- **사전 채택** = Day 4/5/6/7 archive 의 명시 학습 SoT 가 있을 때 (Lakekeeper UUID-prefix, lib reuse 패턴 등)
- **사후 우회** = implementer 가 sanity 단계에서 falsify 한 후 결정 (Day 6 의 `debezium/connect:2.7` tag 미존재 같은 환경 의존)

본 작업 = **사후 우회** (Day 5/6/7 archive 의 명시 SoT 없음, implementer 의 schema cross-check 가 첫 falsify 시점).

### 4.4 학습 — frontend의 API response cross-check 의무

Day 7 PR α 에서는 plan 본문의 응답 cols (`district, gu_code, window_start, area_count, ...`) 가 실 sink DDL 과 1:1 일치 → 사후 우회 미발생 (Day 7 PR γ §2 의 deviation E 미발생 SoT). 본 PR β 에서는 plan type 정의가 PR #45 의 `_row_to_dict` 동작과 불일치 → 사후 우회 발생.

**frontend Item type의 정공 검증** — implementer dispatch prompt 에서 `curl <api> | python3 -m json.tool` 의 실 응답 schema 와 1:1 cross-check 의무 명시 (Day 7 PR γ §10-1 의 통합 검증 SoT 의 frontend 확장형).

## 5. hygiene 2건 (#48 backend + #49 frontend) — scope discipline + 자동화 정착

### 5.1 hygiene backend (#48)

#### (a) main HEAD baseline ruff format 회귀 fix

Day 8 PR α 의 code quality reviewer 시점에 발견 — Day 7 PR α (#41) 머지 시점 통과한 `src/api/deps.py` + `src/api/main.py` + `src/api/routes/hotspots.py` 가 ruff 0.15.12 신규 rule 영향으로 format check FAIL.

본 작업은 PR #45 의 scope discipline 으로 미처리 (PR γ §10-5 SoT) → 별도 hygiene PR 분리. 본 hygiene PR 에서 `ruff format src/api/deps.py src/api/routes/hotspots.py` 적용 (`main.py` 는 PR #45 의 style fix commit `3298fcd` 에 포함된).

#### (b) smoke 검증 runbook 갱신

hotfix PR #46 의 default 변경 (`FLINK_SMOKE_RUN_SECONDS` 600 → 0) 영향:

| 파일 | 변경 |
|---|---|
| `docs/runbook/day3_pyflink.md:65` | "SMOKE_RUN_SECONDS 까지 main 대기" → "FLINK_SMOKE_RUN_SECONDS default 0 (long-running mode), SIGTERM 까지 대기 + 1h heartbeat. smoke 검증 시 명시 export 의무" |
| `docs/runbook/day4_silver_to_gold.md:76` | "default smoke run 600초" → "default = long-running mode, smoke 검증 시 환경변수 명시 export" |

기존 Day 1-7 의 smoke 검증 절차 (default 600 의존) 호환성 깨짐 → runbook 명문화 의무.

### 5.2 hygiene frontend (#49)

`web/.eslintrc.json` 부재 → `pnpm lint` 가 interactive `next lint --init` prompt 진입 → 자동 검증 차단. 본 hygiene 으로 `next/core-web-vitals` extends 1 file 정착:

```json
{
  "extends": ["next/core-web-vitals"]
}
```

`pnpm lint` 의 자동화 정착 효과:
- 향후 reviewer / CI / pre-commit hook 의 lint 자동 검증 가능
- entity escape (`&middot;`, `&larr;`) + key prop + a11y의 자동 회귀 0 보장

### 5.3 학습 — scope discipline 분리 패턴 정공 (3 PR)

본 hygiene 분할의 의의:
- **(a) main HEAD baseline 회귀** = PR #45 scope 외 → 별도 PR 분리
- **(b) smoke runbook 갱신** = hotfix #46 의 후속, 본의 의존성 = PR #46 의 default 변경
- **(c) web/.eslintrc 정착** = PR #47 (frontend) 의 후속, scope 외

3의 자연 경계:
- (a) + (b) = backend → 1 PR (#48)
- (c) = frontend → 1 PR (#49)

scope discipline 의 정공 패턴 — 한 PR = 한 논리 작업 + 자연 경계 분리. Day 7 PR β archive §10-5 SoT (자동화 vs 사용자 수동 분리) 의 frontend / backend 확장형.

## 6. SLO P95 24h 실측 결과 (TBD — 2026-05-12 19:45 fire 후 보강)

### 6.1 측정

at job 2 = 2026-05-12 19:45 fire (Day 7 entry plan 시점 등록, PR δ §5 SoT). 측정 명령:

```bash
uv run --extra flink python -m flink_jobs.slo_metrics --hours 24
# 결과 file: /tmp/slo-24h-<timestamp>.log
```

### 6.2 결과 (TBD)

| 항목 | 값 | 평가 |
|---|---|---|
| count (24h 이내 gold row) | TBD | (예상 ~ 270) |
| p50 seconds | TBD | - |
| p95 seconds | TBD | SLO threshold = 420 (7분) |
| p99 seconds | TBD | - |
| max seconds | TBD | - |
| SLO violated | TBD | true / false |

본 archive의 SLO 부분은 19:45 fire 후 별도 commit 으로 보강 예정.

### 6.3 fire 후 진행

```bash
cat $(ls -t /tmp/slo-24h-*.log | head -1)
# 출력 본 archive §6.2 로 반영 + 별도 commit
```

P95 < 420 통과/위반 결과에 따라:
- **통과** = Phase 1A 의 SLO 부분 일관 달성 → 포트폴리오 SLO 페이지 강화
- **위반** = 원인 진단 (fixture 시차 / streaming 가동의 latency / 측정 시점의 burst 등) + Phase 1B 진입 전 fix 의무

## 7. 학습 패턴 5종

### 7-1. dbt-duckdb python model 의 cross-ref + `to_view` 패턴 (신규 자산)

본 archive §1 SoT — `dbt.ref('other_model').to_view('view_name', replace=True)` 패턴이 다른 dbt python model 의 ref 사용 시 필수. Relation 객체의 f-string interpolation 에서 발생하는 데이터 dump 회피.

Phase 1B / 2의 dbt mart 가 다른 dbt python model 의 ref 사용 시 본 패턴 reuse.

### 7-2. `lib/lifecycle.py` 추출 = streaming long-running의 단일 SoT

본 archive §3 SoT — 3 streaming job 의 동일 `time.sleep(SMOKE_RUN_SECONDS)` 패턴 → `wait_for_shutdown()` 위임. 다음의 정공:

- silent exit fingerprint 회피 (1h heartbeat + SIGTERM graceful)
- `FLINK_SMOKE_RUN_SECONDS=0` default 의 long-running mode + `>0`의 smoke mode 양립
- Phase 1B 신규 streaming job 추가 시 reuse 의무

본 lib reuse 패턴 — `lib/duckdb_iceberg` (PR #28) 의 정공 reuse 패턴 SoT 와 정합. 별도 lib (`lib/lifecycle`) 정착의 첫 사례.

### 7-3. Plan 본문 vs 실 응답 schema cross-check 의 frontend 확장

본 archive §4 SoT — Day 7 PR γ §10-1 의 "통합 검증 single source" 패턴이 frontend Item type에 확장:

1. implementer dispatch 시 `curl <api> | python3 -m json.tool` 의 실 응답 schema cross-check 의무
2. Plan 본문의 type 정의 vs 실 응답 keys 1:1 비교
3. 불일치의 사후 우회 (PR γ §10-2 SoT)

Day 7 PR α 의 Deviation E 미발생과 대조 — 실 응답 schema의 사후 우회는 매 PR 마다 검증 의무 (자동으로 미발생 가정 X).

### 7-4. hygiene의 scope discipline 분리 (3 → 2 PR)

본 archive §5 SoT — hygiene 분할 = 자연 경계 분리. PR scope discipline 의 정공:

- main HEAD baseline 회귀 (별도 PR 분리)
- 의존성 (hotfix #46 의 default 변경 → runbook 갱신)
- 자동화 정착 (web/.eslintrc.json)

Day 7 PR β archive §10-5 의 "자동화 vs 사용자 수동 분리" SoT 의 backend / frontend 확장형.

### 7-5. Plan 의 implementer dispatch + reviewer 분리 패턴 정공 (Day 8 5 PR)

본 Day 8의 5 PR 모두 다음 패턴 정공 reuse:

| PR | implementer | spec reviewer | code quality reviewer | PR 생성 |
|---|---|---|---|---|
| #45 PR α | ✅ Task 8.1 + 8.2 통째 | ✅ 통합 검증 직접 재현 | ✅ Minor 2건 발견 | ✅ |
| #46 hotfix | (메인 자율, LOC 작음) | - | - | ✅ |
| #47 PR β | ✅ Task 8.3 | - | ✅ 모든 항목 PASS | ✅ |
| #48 hygiene-backend | (메인 자율) | - | - | ✅ |
| #49 hygiene-frontend | (메인 자율) | - | - | ✅ |

Implementer + reviewer 분리 의무 = LOC에 따라 결정:
- 본격 코드 (>200 LOC) = implementer + reviewer 분리
- hygiene / hotfix (<50 LOC) = 메인 자율 + 직접 검증

Day 7 PR γ §10-1 의 통합 검증 SoT + execution-policy 메모리의 "200 LOC 기준 분할" 정공 reuse.

## 8. 관련 문서

- 후속 archive (Cloudflare 배포 + 스크린샷): [`2026-05-12-day-8-cloudflare-deploy.md`](2026-05-12-day-8-cloudflare-deploy.md)
- Day 7 archive 2건:
  - [`2026-05-11-day-7-nextjs-cloudflare-deploy.md`](2026-05-11-day-7-nextjs-cloudflare-deploy.md) — Day 7 PR γ (Next.js + Cloudflare 자동화 docs)
  - [`2026-05-11-day-7-streaming-smoke-timeout-restart.md`](2026-05-11-day-7-streaming-smoke-timeout-restart.md) — Day 7 PR δ (streaming SMOKE_RUN_SECONDS + uv extra)
- Day 4/5/6 archive (lib reuse SoT + python model SoT):
  - [`2026-05-09-day-4-tasks-4_1-4_3.md`](2026-05-09-day-4-tasks-4_1-4_3.md) — Lakekeeper UUID-prefix path SoT
  - [`2026-05-10-day-5-dbt-iceberg-compat.md`](2026-05-10-day-5-dbt-iceberg-compat.md) — dbt python model 정착
  - [`2026-05-11-day-6-airflow-cdc-integration.md`](2026-05-11-day-6-airflow-cdc-integration.md) — Day 6 5종 학습 패턴
- 관련 PR: #45 (PR α), #46 (hotfix), #47 (PR β), #48 (hygiene backend), #49 (hygiene frontend)
- Plan: `docs/superpowers/plans/phase-1a-week-2.md` Day 8 Task 8.1-8.3 (line 1567-2037)
- spec: `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` §6-1 Day 8
- 메모리: `phase-1a-progress`, `execution-policy`, `korean-conventions`, `project-identity-correction`

## 9. SLO 부분 보강 시점 (TBD)

본 archive §6 의 SLO 부분은 2026-05-12 19:45 at job 2 fire 후 별도 commit 으로 보강 예정. 보강 시점에 본 §9도 갱신.
