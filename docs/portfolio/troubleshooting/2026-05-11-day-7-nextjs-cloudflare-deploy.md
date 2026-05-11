# Day 7 — Next.js 메인 지도 + FastAPI + Cloudflare 배포 자동화 영역 트러블슈팅

> 작성: 2026-05-11
> 영역: Day 7 진입 직후 Phase 0 (producer/streaming 재가동) + PR α (Task 7.1 + 7.2) + PR β (Task 7.3 + 7.4 자동화)
> 관련 PR: #41 (PR α), #42 (PR β), 본 PR γ
> 운영 runbook: [`day7_deploy.md`](../../runbook/day7_deploy.md) (Cloudflare Pages + Tunnel 배포 점검 항목)

## 0. 진입 흐름 요약

Phase 1A Day 6 종료 (PR #33~40, main HEAD `edb7c02`) 후 Day 7 (메인 지도 + Cloudflare Pages 배포) 진입. entry plan 단계:

| Phase | 산출 | PR |
|---|---|---|
| 0 | hotspot/subway producer + bronze_to_silver / silver_to_gold streaming 재가동, SLO P95 < 7분 24h 측정 시작 | (PR 없음) |
| A | Next.js 14 골격 + FastAPI `/api/hotspots` | PR #41 |
| B | Leaflet 메인 지도 + 자치구 색상 마커 | PR #42 commit `c02b0e3` |
| B' | next 14.2.15 → 14.2.35 보안 patch | PR #42 commit `9dd702e` |
| C | Cloudflare Pages + Tunnel 자동화 docs | PR #42 commit `b377997` |
| γ | Day 7 archive + runbook 보강 + memory 갱신 | 본 PR |

진입 전 사전 결정 2건:
- **deviation 7.2-A·B 사전 채택** — Day 4/5/6 학습 SoT (Lakekeeper UUID-prefix path + `flink_jobs.lib.duckdb_iceberg` 우회 패턴) 가 있어, plan 본문의 inline SECRET DDL + `iceberg_scan(s3://...)` 직접 호출을 implementer dispatch 전에 우회 패턴으로 채택. 검증 1회 부담 없이 학습 정착.
- **자동화 영역 vs 사용자 수동 영역 분리** — Task 7.4 의 Step 4 (wrangler / cloudflared 실 명령) 는 사용자 토큰 의존이라 docs 인용만. 본 세션 commit 영역은 config 템플릿 + 절차 docs + .gitignore 만.

## 1. Issue 1 — pnpm 11 의 `unrs-resolver` ignored build script 정책 변경 (PR #41 hygiene `4ee8dd1`)

### 증상

Task 7.1 (Next.js scaffold) 의 9 file 작성 후 첫 `cd web && pnpm install` 은 정상 종료 (`383 packages installed`). 그러나 직후 `pnpm typecheck` 실행 시:

```
[ERR_PNPM_IGNORED_BUILDS] Ignored build scripts: unrs-resolver@1.11.1.
```

`pnpm exec` 가 deps lifecycle 재검증 단계에서 exit 1 차단. `./node_modules/.bin/tsc --noEmit` 직접 호출은 즉시 통과 → plan 코드 결함 아님.

### 원인

pnpm 11 의 정책 변경. `unrs-resolver` (rust binary) 같은 lifecycle script 가 있는 deps 는 명시적 승인 없이는 build 차단. plan 본문 (Step 8) 의 `corepack enable pnpm` 기반 흐름은 pnpm 9 가정 — pnpm 11 의 정책 차단을 plan 이 예측 못함.

`unrs-resolver` 출처: `eslint-config-next@14.2.15` → `unrs-resolver` (TypeScript path alias resolver 의 rust 가속 binary).

### 해결

`pnpm approve-builds` 1회 실행:
- 자동 부산물 `web/pnpm-workspace.yaml` 생성:
  ```yaml
  allowBuilds:
    unrs-resolver: false
  ```
- `false` 의 의미 = native binary build 차단 (fallback path 로 정상 작동). status 정리 후 `pnpm typecheck` exit 0.

별도 hygiene commit (`4ee8dd1`) 으로 정착:
- `web/pnpm-workspace.yaml` commit — CI/fresh clone 시 동일 차단 회피
- `web/next-env.d.ts` commit — Next 공식 권장 (`tsconfig.json` 의 include 에 명시됨)
- `.gitignore` 에 `*.tsbuildinfo` 추가 — tsc incremental cache 영구 제외

### 학습

- **homebrew 의 Node 25 는 corepack 분리** — Node 16.10+ 가 corepack 을 bundle 하지만 homebrew 의 node 25.x 부터 corepack 이 별도 formula 로 분리. plan 본문의 `corepack enable pnpm` 그대로 따라가면 `command not found`. 본 프로젝트는 `brew install pnpm` 직접 설치로 우회.
- **pnpm 11 의 build script 검증 정책** — Phase 1B 진입 시 추가 native binary 도입 시 같은 패턴 재발 가능. pnpm-workspace.yaml 의 `allowBuilds` 가 single source.

## 2. Issue 2 — plan 코드 보강 7.2-A·B 사전 채택 (PR #41 commit `682dcd4`)

### 증상

증상 없이 사전 채택. Day 4 archive `2026-05-09-day-4-tasks-4_1-4_3.md` §plan deviation 4 (Lakekeeper REST UUID-prefix path 미해결) + PR #28 (lib 추출) + Day 5 `stg_hotspot_silver.py` + Day 6 `dim_place.py` 의 학습 SoT 가 plan 본문의 가정과 충돌.

### 원인

plan 본문 (Task 7.2 line 1125~1158, 1183, 1213):

```python
# src/api/deps.py (plan 원안)
con.execute(f"""CREATE OR REPLACE SECRET (
    TYPE S3, KEY_ID '{s.minio_user}', SECRET '{s.minio_password.get_secret_value()}',
    ENDPOINT '{endpoint_no_proto}', URL_STYLE 'path', USE_SSL false, REGION '{s.minio_region}'
)""")
```

```python
# src/api/routes/hotspots.py (plan 원안)
con.execute(f"""SELECT ... FROM iceberg_scan('{base}/gold/fact_hotspot_congestion_5min')""")
```

문제 2건:
- inline SECRET DDL — SQL injection 표면 (single quote escape) 이 module 마다 흩어짐. PR #28 의 `_quote_literal` 일원화와 충돌
- `iceberg_scan(s3://...)` — Day 4 학습대로 Lakekeeper REST 가 vend 하는 UUID-prefix path 를 resolve 못함

### 해결 — 사전 채택 (implementer dispatch 전)

| Plan 원안 | 채택 | 출처 |
|---|---|---|
| inline SECRET DDL | `flink_jobs.lib.duckdb_iceberg.configure_duckdb(con)` 위임 | PR #28 lib 추출, Day 4 archive |
| `iceberg_scan('s3://...')` | `table_paths(catalog, "silver.hotspot_congestion")` + `read_parquet({paths!r}, hive_partitioning=true)` + 빈 path list fast-path | Day 5 / Day 6 정착 우회 패턴 |
| `from src.api.X` | `from api.X` (pyproject hatch packages 에 `src/api` 추가) | flink_jobs / platform_common / producers 와 동일 패턴 |

implementer 가 dispatch prompt 의 deviation 가이드 따라 즉시 적용. 검증 1회 부담 없이 통과.

### 부수 — deviation E (응답 schema) 미발생

plan 의 `/api/hotspots` cols (`district, gu_code, window_start, area_count, avg_congest_score, max_congest_score`) + `/api/hotspots/areas` cols (`area_code, area_name, district, latitude, longitude, congest_level_score, congest_level, api_response_ts`) 가 실제 `silver_to_gold.py` line 51~63 + `bronze_to_silver.py` line 89~109 의 sink DDL 과 1:1 일치 확인. 보강 불필요.

implementer 의 schema cross-check 가 deviation 후보 처리의 정공. 후보를 사전 명시했지만 실 검증 후 미발생 결정.

### 학습

- **plan 코드 보강의 사전 채택 vs 사후 우회 — 판단 기준**:
  - 사전 채택 = Day 4/5/6 archive 의 명시 학습 SoT 가 있을 때 (Lakekeeper UUID-prefix, lib reuse 패턴 등)
  - 사후 우회 = implementer 가 sanity 단계에서 falsify 한 후 결정 (Day 6 의 `debezium/connect:2.7` tag 미존재 같은 환경 의존)
  - 본 PR α 는 전자, plan 코드 보강 표 + commit body 에 명시
- **`flink_jobs.lib.duckdb_iceberg` 의 4번째 consumer 정착** — PR #28 추출 후 stg_hotspot_silver (Day 5) / dim_place (Day 6) / scripts/duckdb_check + slo_metrics (PR #28) / FastAPI deps.py (Day 7) 까지 5개 consumer. drift 위험 0.

## 3. Issue 3 — import path 통일 (PR #41 commit `682dcd4`)

### 증상

plan 본문은 `from src.api.X import Y` 형식. 그러나 mypy 가 `Source file found twice under different module names: "src.api.X" and "api.X"` 류 충돌 가능성 (다른 src/ 영역 import 와 mix).

### 원인

본 프로젝트 컨벤션 (직전 PR #28 정착): `pyproject.toml` 의 `[tool.hatch.build.targets.wheel].packages = ["src/platform_common", "src/producers", "src/flink_jobs"]` + `from <pkg>` 직접 import. plan 본문은 이 컨벤션과 정합 안 됨 (`src.api` 형식이 다른 module 과 다른 prefix).

### 해결

implementer 가 자율 결정:
- `pyproject.toml [tool.hatch.build.targets.wheel].packages` 에 `src/api` 추가
- 모든 import 를 `from api.X import Y` 로 통일 (`src/api/main.py`, `src/api/routes/hotspots.py`, `tests/integration/test_api_hotspots.py`)

uv.lock 갱신 +286 line (hatch packages 변경 영향).

### 학습

- **plan 본문의 import path 같은 micro convention 도 검증 필요** — Day 6 학습 패턴 #3 (plan 본문 코드는 검증 안 된 가정) 의 적용 범위. implementer 가 컨벤션 정합성 자율 판단해야 할 영역.
- **mypy 의 "Source file found twice" 회피** — hatch packages 의 src/ subdir 등록 + import path 통일 한 곳에서만 처리.

## 4. Issue 4 — NEXT_PUBLIC_API_BASE build-time inline 의 dev/prod 분기 (PR #42 commit `c02b0e3` + `b377997`)

### 증상

Task 7.3 implementer 가 발견. `next.config.mjs` 의 `process.env.NEXT_PUBLIC_API_BASE || 'http://localhost:8000'` 흐름이 static export 빌드 시점에 chunk 안으로 inline 됨:

```bash
$ NEXT_PUBLIC_API_BASE 미지정 채로 pnpm build
$ grep -r "localhost:8000" web/out/_next/static/chunks/*.js | head -3
# web/out/_next/static/chunks/467-xxx.js: ...fetch("http://localhost:8000/api/hotspots/areas")...
```

→ prod 배포 시 build env 누락하면 Cloudflare Pages 의 chunk 가 `localhost:8000` 을 fetch 시도 → mixed content 차단.

### 원인

Next.js 14 static export 의 design — `NEXT_PUBLIC_*` env 는 build time 에 chunk 로 inline. runtime injection 없음. dev 환경의 fallback 이 prod build 에 그대로 남으면 위험.

### 해결

- **`infra/cloudflare/README.md`** — "Pages 정적 사이트 → Build-time env 의무 (중요)" 섹션 신설. dev/prod env 분기 표 (dev = `localhost:8000` fallback, prod = `https://api.<your-domain>.com` Tunnel) + 빌드 방법 2가지 (wrangler env 또는 `.env.production` 파일)
- **`docs/runbook/day7_deploy.md`** — 점검 항목 3 + 환경 분기 빠른 참조 표 + grep 검증 명령:
  ```bash
  grep -r "localhost:8000" web/out/ 2>/dev/null | head -3
  # prod build 환경에서는 0 hit 기대. 1건 이상 시 build env 누락 → 재빌드.
  ```

### 학습 (reviewer 의 실증 입증)

PR β code quality reviewer 가 default `pnpm build` (env 미지정) 로 fallback `localhost:8000` 1 hit 재현 → runbook 점검 항목 3 의 grep 절차가 실제 위험을 잡아냄 입증.

- **docs 점검 항목의 사실 정합성 = 직접 재현으로만 입증** — reviewer 가 단순 grep 통과 확인이 아니라 실 환경에서 위험 시나리오 재현 후 docs 와 일치 확인.
- **Day 6 학습 패턴 #5 (archive = future 자산) 의 확장형** — runbook 점검 항목 자체가 future 디버깅 자산. reviewer 의 실증이 그 정합성을 보존.

## 5. Issue 5 — next 14.2.15 보안 권고 + 14.2.35 patch bump (PR #42 commit `9dd702e`)

### 증상

PR α code quality reviewer 가 Minor 로 권고:
- next 14.2.15 = Vercel 2025-12-11 보안 권고 (image optimization SSRF / authorization bypass) 의 fix 라인 14.2.21+ 미만
- 본 PR 의 `output: 'export'` + `images.unoptimized: true` 라 실 노출 표면 0 이지만 sustainability 차원에서 bump 권고

### 해결

별도 commit `9dd702e`:
- `web/package.json` 의 `"next": "14.2.15"` + `"eslint-config-next": "14.2.15"` → 둘 다 `14.2.35` (14.2.x latest patch)
- 메이저/마이너 bump 회피 (14.3.x / 15.x 안정성 우선)
- `pnpm-lock.yaml` diff scope = next + eslint-config-next + @next/* sub-packages 만, 외부 deps 동시 bump 없음

### 학습

- **14.2.x 의 alias 미존재** — Debezium `2.7` 처럼 major.minor alias 가 안 박힌 라이브러리도 있음. npm view 또는 dist-tags 로 latest patch 확인 필수. plan 본문의 deps 버전은 plan 시점 기준, 보안 권고에 따라 patch bump 는 별도 commit.
- **`@next/swc-*` 14.2.33 pin** — next 14.2.35 가 swc 를 14.2.33 으로 npm 메타데이터 pin. 정상 publish 패턴, 외부 bump 아님.

## 6. Issue 6 — HTML entity escape (JSX safe) (PR #42 commit `c02b0e3`)

### 증상

plan 본문 (Task 7.3 line 1387~1390) 의 raw 문자:

```tsx
<div className="text-xs">
  <strong>{a.area_name}</strong> · {a.district}<br />
  ...
</div>
```

React `react/no-unescaped-entities` 규칙은 `'`, `"`, `>`, `}` 같은 entity 를 unescaped 로 박으면 경고. `·` 같은 middle dot, `→` arrow, `©` copyright 도 같은 패턴.

### 해결

implementer 가 JSX entity 로 치환:
- `·` → `&middot;`
- `→` → `&rarr;`
- `©` → `&copy;` (footer 의 copyright)
- `<br />` 는 그대로 (self-closing void element, 정상)

렌더링 결과 동일 (브라우저가 entity 자동 디코드).

### 학습

- **plan 본문 코드는 lint 통과를 보장 안 함** — Day 6 학습 패턴 #3 (plan 본문 코드는 검증 안 된 가정) 의 frontend 사례. ruff (Python) 만 적용해온 baseline 에서 `pnpm lint` / `eslint-config-next` 같은 frontend lint 도 본 PR 시점에 사실상 첫 통과.
- **JSX entity vs React 자동 escape 의 충돌 회피** — React 가 자동으로 처리하는 영역 (외부 데이터 `{a.area_name}`) 과 unescaped 경고 영역 (raw text 의 entity 문자) 의 책임 분리.

## 7. Issue 7 — CORS wildcard 좁히기 TODO docs only (PR #42 commit `b377997`)

### 증상

PR α 의 `src/api/main.py:25` 의 `allow_origins=["*"]` wildcard 가 Phase 1A 단계 적정 (PR α reviewer 판단). Cloudflare Pages 도메인 확정 후 좁히기 필요.

### 해결

PR β 영역에서 **코드 수정 0건**:
- `infra/cloudflare/README.md` 에 "CORS 정책 (TODO)" 섹션 명시 — "현재 wildcard, Phase 1B 진입 전 별도 task"
- `src/api/main.py` 수정 안 함 (PR β scope discipline)

### 학습

- **TODO 의 docs 명시 vs code 명시** — code 안의 TODO comment 는 stale 위험. docs 의 별도 섹션이 sustainability 보존. PR β reviewer 가 scope discipline (코드 수정 0, TODO 만 docs) 정착 확인.

## 8. Issue 8 — Cloudflare 토큰/도메인 placeholder 의무 (PR #42 commit `b377997`)

### 증상

`infra/cloudflare/tunnel-config.example.yml` + `infra/cloudflare/README.md` + `docs/runbook/day7_deploy.md` 가 실 token / UUID / 도메인을 박으면 git history 영구 누출.

### 해결

placeholder 만 사용:
- `<YOUR_TUNNEL_UUID>` — Cloudflare Tunnel UUID
- `<you>` — macOS 사용자명 (`/Users/<you>/.cloudflared/`)
- `<your-domain>` — 본인 소유 도메인

`.gitignore` 에 차단 추가:
```
# Cloudflare
.cloudflared/
infra/cloudflare/credentials/
infra/cloudflare/tunnel-config.yml
```

`git check-ignore -v infra/cloudflare/tunnel-config.yml infra/cloudflare/credentials/foo.json` 으로 차단 정착 검증 (PR β code reviewer).

### 학습

- **example.yml 의 placeholder 컨벤션** — 실 값 박힌 `tunnel-config.yml` 은 차단, `.example.yml` 만 commit. PR γ 에서 사용자 실 배포 후 결과 docs 에도 placeholder 유지 의무.

## 9. 부수 발견 — 운영 측면

### 9-1. Phase 0 producer + streaming 재가동의 통합 검증 single source

PR α 검증 단계에서 hotspot/subway producer + bronze_to_silver / silver_to_gold streaming 4 process 호스트 백그라운드 가동 (PID 32598/32599/32619/32620). Day 4 archive 의 "5분 가동 후 새 +3 row 정확히 incremental" 패턴 reuse. bronze 188 → 369 (+181 row), silver/gold 정상 commit.

deviation #3 (SLO P95 < 7분 24h 실측) 의 첫 step. 24h 누적 후 (2026-05-12) `uv run python -m flink_jobs.slo_metrics --hours 24` 측정 예정.

### 9-2. gold mart 의 watermark + tumbling 윈도우 close 지연

Task 7.2 implementer 가 발견. silver 는 최신 (`2026-05-11 09:55` api_response_ts) 인데 gold 는 `2026-05-09 02:30` window_start. silver_to_gold 의 watermark + tumbling 윈도우가 다음 이벤트 도착까지 close 대기.

- `/api/hotspots/areas` (silver 직접 read) — count=3, 최신 데이터 OK
- `/api/hotspots` (gold read) — count=3, district 평균 1.0 (이틀 전 데이터)

Day 7 의 메인 지도 source 는 `/api/hotspots/areas` (area-level) 라 직접 영향 없음. district 평균 색상 표현이 필요해지면 gold 의 stale 도 고려 — Phase 1B 진입 시점에 검토.

### 9-3. `useEffect([])` 의 1회 fetch — 데이터 신선도 한계

PR β code quality reviewer 의 Minor M4. Gold mart 가 5분 주기 streaming 갱신이지만 frontend 는 page reload 까지 stale. SLO "데이터 신선도 P95 < 7분" 의 endpoint 는 서버 (API) 까지이므로 spec 위반 아님. Phase 1B `user.events.v1` 도입 시 SSE / poll 옵션 검토.

## 10. 학습 패턴 5종

### 10-1. subagent-driven development 의 통합 검증 single source (Day 6 패턴 1 확장)

Day 6 의 manual trigger 1회 = 통합 검증 single source 패턴을 Day 7 에 확장 적용:
- PR α — curl 3종 (`/health` + `/api/hotspots` + `/api/hotspots/areas`) 가 uvicorn 가동 후 통합 검증
- PR β — uvicorn + Next dev 동시 가동 후 FastAPI count=3 회귀 + Next SSR header 매칭

reviewer 가 implementer 보고 신뢰 X 직접 재현. PR α 의 spec reviewer 가 curl 3종 + pytest 2 PASS + ruff/mypy 회귀 0 모두 재실행 통과. PR β 의 spec reviewer 도 동일 통합 검증 재실행.

### 10-2. plan 코드 보강의 사전 채택 vs 사후 우회

판단 기준:
- **사전 채택** = Day 4/5/6 archive 의 명시 학습 SoT 가 있을 때 (Lakekeeper UUID-prefix, lib reuse 패턴 등). implementer dispatch 전에 deviation 가이드 prompt 에 박음. 검증 1회 부담 없이 학습 정착.
- **사후 우회** = implementer 가 sanity 단계에서 falsify 한 후 결정 (Day 6 의 `debezium/connect:2.7` tag 미존재 같은 환경 의존).

Day 7 의 deviation 7.2-A·B 는 전자 (Day 4 archive SoT). deviation E 는 후자 (implementer 의 schema cross-check 후 미발생 결정).

### 10-3. reviewer 의 실증 입증 (Day 6 패턴 5 확장)

PR β code quality reviewer 가 단순 grep 통과 확인이 아니라:
1. default `pnpm build` (env 미지정) 실 실행
2. `web/out/_next/static/chunks/*.js` 에 fallback `localhost:8000` 1 hit 확인
3. runbook 점검 항목 3 의 grep 절차가 실제 위험 시나리오를 잡아냄 입증

docs 점검 항목의 사실 정합성 = 직접 재현으로만 입증. Day 6 archive 의 future 자산 패턴이 docs 점검 항목 자체까지 확장.

### 10-4. lib reuse 패턴의 5번째 consumer 정착

PR #28 (lib 추출) 후 `flink_jobs.lib.duckdb_iceberg.{build_catalog, configure_duckdb, table_paths}` 의 consumer:

| consumer | PR | 영역 |
|---|---|---|
| `stg_hotspot_silver.py` (dbt python model) | PR #29 (Day 5) | dbt staging |
| `dim_place.py` (dbt python model) | PR #38 (Day 6) | dbt gold |
| `scripts/duckdb_check.py` | PR #28 (Day 5 진입 전) | 운영 점검 |
| `flink_jobs/slo_metrics.py` | PR #28 (Day 5 진입 전) | SLO 측정 |
| `src/api/deps.py` (FastAPI) | PR #41 (Day 7) | API serving |

5개 consumer + SQL injection 표면 lib 한 곳에서 처리. drift 위험 0.

### 10-5. 자동화 영역 vs 사용자 수동 영역 분리 (Cloudflare 배포)

Task 7.4 의 책임 분리:
- **자동화 영역** (본 PR commit) — config 템플릿 (`tunnel-config.example.yml`) + 절차 docs (`infra/cloudflare/README.md`, `docs/runbook/day7_deploy.md`) + `.gitignore` 차단
- **사용자 수동 영역** — 실 토큰 발급 (`cloudflared tunnel login`) + 도메인 결정 + 실 배포 (`wrangler pages deploy`, `cloudflared tunnel run`) + Cloudflare Pages 환경변수 등록

본 세션은 자동화 영역만 처리, 사용자 토큰/도메인 의존은 docs 인용. PR β reviewer 가 scope discipline 정착 확인. Phase 1B 의 D1 + Workers Pages Functions 도입 시점에 같은 분리 패턴 reuse.

## 11. 관련 문서

- 운영 runbook: [`day7_deploy.md`](../../runbook/day7_deploy.md) — 배포 점검 항목 7개 + 환경 분기 빠른 참조 + 미달 대응
- 배포 가이드: [`infra/cloudflare/README.md`](../../../infra/cloudflare/README.md) — Pages + Tunnel + ngrok fallback + Build-time env 의무 + CORS TODO + dev/prod env 분기
- spec §3 (Cloudflare Pages + Tunnel) / §6-1 Day 7 (메인 지도 종료 게이트) / §9-1 Day 7 fallback (ngrok)
- Phase 1A Week 2 plan §Day 7 Task 7.1 + 7.2 + 7.3 + 7.4
- 직전 archive (학습 SoT 단일 출처):
  - `2026-05-09-day-4-tasks-4_1-4_3.md` — Lakekeeper REST UUID-prefix path 학습 + pyiceberg `plan_files()` 우회 패턴
  - `2026-05-10-day-5-dbt-iceberg-compat.md` — dbt python model + lib 우회 패턴 정착
  - `2026-05-11-day-6-airflow-cdc-integration.md` — Day 6 5종 학습 패턴 (manual trigger / env override / plan 가정 / dbt-duckdb 패턴 / archive 자산)
- Vercel 보안 권고: https://nextjs.org/blog/security-update-2025-12-11 (image optimization SSRF / authorization bypass, fix 라인 14.2.21+)
