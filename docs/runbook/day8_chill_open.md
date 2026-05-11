# Day 8 — chill-open 데모 + Cloudflare 실 배포 운영 매뉴얼

PR #45~50 머지 후의 chill-open 데모 (정적 인허가 + dbt mart + API + Next.js 페이지) + Cloudflare Pages 실 배포 + 임시 Tunnel 의 평소 기동 / 진단 / mitigation 운영 절차.

> **archive SoT** — 본 runbook 의 학습 자산은 다음 archive 2건 참조:
> - [`../portfolio/troubleshooting/2026-05-12-day-8-archive.md`](../portfolio/troubleshooting/2026-05-12-day-8-archive.md) — Day 8 코드 학습 5건 + SLO 24h TBD
> - [`../portfolio/troubleshooting/2026-05-12-day-8-cloudflare-deploy.md`](../portfolio/troubleshooting/2026-05-12-day-8-cloudflare-deploy.md) — Cloudflare 배포 학습 7건 + 스크린샷 12장

## 사전 조건

- Day 7 runbook ([`day7_deploy.md`](./day7_deploy.md)) 의 Cloudflare 자동화 docs 정착
- Day 6 runbook ([`day6_cdc.md`](./day6_cdc.md)) 의 CDC + dim_place 환경 정착 (silver.dim_place 8 rows)
- Day 5 runbook ([`day5_dbt.md`](./day5_dbt.md), [`day5_airflow.md`](./day5_airflow.md)) 의 dbt + Airflow 정착
- Day 4 runbook ([`day4_silver_to_gold.md`](./day4_silver_to_gold.md)) 의 streaming + SLO + DuckDB 검증
- Day 3 runbook ([`day3_pyflink.md`](./day3_pyflink.md)) 의 PyFlink JAR + JDK 17 + Lakekeeper warehouse
- Day 1 runbook ([`day1_infra.md`](./day1_infra.md)) 의 docker compose 4종 healthy
- Cloudflare 계정 가입 + GitHub OAuth 권한 부여 (Day 8 시점에 사용자가 진행)
- cloudflared 설치 (`brew install cloudflared`)

## 본 runbook 의 사용 시점

다음 작업 / 증상 마주치면 본 runbook 의 절차 진입:

- chill-open 데모 화면 작동 검증 (`/chill/` 페이지 + Leaflet 메인 지도)
- 공공 인허가 정적 데이터 1회 적재 (`scripts/load_static_places.py`)
- `chill_open_now` dbt mart 빌드 + 5 test 검증
- `/api/chill-open` 엔드포인트 응답 검증
- Cloudflare Pages 실 배포의 build / deploy 진단 (chunk 500 / 502 transient / asdf NODE_VERSION / Pages project 재생성)
- 임시 Cloudflare Tunnel 가동 + Pages 환경변수 갱신
- streaming long-running mode 검증 (hotfix PR #46 의 `lib/lifecycle.wait_for_shutdown()`)
- 다음 증상 마주침:
  - Cloudflare Pages 의 `Uploaded X files (Y already uploaded)` 에서 Y > 0 (chunk dedup cache)
  - 브라우저의 `<script> 로드에 실패` (chunk 500)
  - `NODE_VERSION=20` asdf preset fail (`20.20.0` 자동 resolve)
  - cloudflared 재가동 후 새 hostname 발급 → Pages 환경변수 갱신 필요

## 평소 기동

### 0. Python 환경 + Flink JAR + docker compose (Day 3 runbook 통과)

```bash
uv sync --extra dev --extra flink                       # 의존성 일괄 (단 dev/flink mutual exclusive, PR δ §6-2 SoT)
ls infra/flink/jars/                                    # 5 JAR 확인
uv run --with httpx python infra/lakekeeper/bootstrap.py   # warehouse 멱등
docker compose ps                                       # 4 healthy
```

**주의** — `uv sync --extra dev --extra flink` 는 protobuf 충돌로 mutual exclusive (PR δ §6-2 SoT). 실제로는 둘 중 하나만 install (streaming 가동 = `--extra flink`, dbt / pytest = `--extra dev`). review subagent dispatch 시 의무 사항.

### 1. Postgres `places` seed + Debezium connector (Day 6 runbook 통과)

```bash
uv run python -m producers.places_seeder           # 5 row seed (Day 6 Task 6.2)
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @infra/kafka-connect/debezium-postgres.json
```

silver.dim_place 8 rows (`r=5` + `u=3`) 확인.

### 2. 공공 인허가 정적 데이터 1회 적재 (Day 8 Task 8.1)

```bash
uv run python scripts/load_static_places.py
```

출력:
```
loaded 10 rows from data/reference/places_seed_sample.csv
wrote parquet: s3://seoul-warehouse/warehouse/bronze/places_static_v1/data.parquet
```

검증:
```bash
uv run python -c "
import duckdb
from flink_jobs.lib.duckdb_iceberg import configure_duckdb
con = duckdb.connect()
configure_duckdb(con)
print(con.execute(\"SELECT count(*) FROM read_parquet('s3://seoul-warehouse/warehouse/bronze/places_static_v1/data.parquet')\").fetchone())
"
# (10,)
```

본 적재는 **1회 작업** — Day 8 진입 시 1회 실행 후 영구 보존. Iceberg 정식 등록은 Day 9 Spark `MIGRATE` 또는 `CREATE TABLE LIKE` 에 묶음 처리 예정.

### 3. dbt run + test (Day 5 runbook 통과)

```bash
cd dbt/seoul
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt run
DBT_PROFILES_DIR=$(pwd) uv run --project ../.. dbt test
cd ../..
```

출력:
```
dbt run: Done. PASS=4   (dim_place, stg_hotspot_silver, chill_open_now, fact_hotspot_congestion_hourly)
dbt test: Done. PASS=17 (11 기존 + 6 신규 chill_open_now schema)
```

### 4. streaming 4 process 가동 (long-running mode, hotfix #46)

```bash
# producer 2 (5분 / 1분 polling)
nohup uv run python -m producers.hotspot_producer > /tmp/hotspot.log 2>&1 & disown
nohup uv run python -m producers.subway_producer > /tmp/subway.log 2>&1 & disown

# streaming 2 (long-running default, FLINK_SMOKE_RUN_SECONDS=0)
nohup uv run --extra flink python -m flink_jobs.bronze_to_silver > /tmp/b2s.log 2>&1 & disown
nohup uv run --extra flink python -m flink_jobs.silver_to_gold > /tmp/s2g.log 2>&1 & disown
```

**hotfix #46 의 default 변경** — `FLINK_SMOKE_RUN_SECONDS` 환경변수 미설정 시 long-running mode (SIGTERM 까지 대기 + 1h heartbeat). smoke 검증 (10분~30분 짧은 가동) 은 명시 export:

```bash
FLINK_SMOKE_RUN_SECONDS=600 nohup uv run --extra flink python -m flink_jobs.bronze_to_silver > /tmp/b2s.log 2>&1 & disown
```

5분 텀블링 윈도우 close 는 최소 10분+ 가동 필요.

### 5. FastAPI uvicorn 가동 (Day 7 PR α)

```bash
nohup uv run uvicorn api.main:app --host 0.0.0.0 --port 8000 > /tmp/uvicorn.log 2>&1 & disown
sleep 3
curl -sf http://localhost:8000/health           # {"ok":true}
curl -sf http://localhost:8000/api/chill-open | python3 -m json.tool | head -25
```

`/api/chill-open` 응답 예상:
```json
{
  "items": [
    {
      "biz_reg_no": 2208612001,
      "name": "강남 24시 김밥",
      "category": "음식점",
      "district": "강남구",
      "latitude": 37.4979,
      "longitude": 127.0271,
      "open_hour": 0,
      "close_hour": 24,
      "avg_congest_score": 1.0,
      "is_open_now": true
    },
    ...
  ],
  "count": 3,
  "current_hour": 0
}
```

### 6. Next.js dev / static export 검증

```bash
cd web
pnpm install --frozen-lockfile
pnpm typecheck                                  # exit 0
pnpm lint                                       # No ESLint warnings or errors (PR #49 정착)
pnpm build                                      # 4 Route Static prerender
ls -la out/chill/                               # index.html 존재 확인
```

또는 dev 모드:
```bash
cd web && pnpm dev &                           # localhost:3000
sleep 3
curl -sf http://localhost:3000/chill/ | grep -c "한가하고 영업 중"   # 1+
```

### 7. Cloudflare 실 배포 (사용자 수동 작업)

#### 7-A. 임시 Tunnel 가동 (Phase 1A 데모, 비영구)

```bash
nohup cloudflared tunnel --url http://localhost:8000 > /tmp/cloudflared.log 2>&1 & disown
sleep 8
grep "trycloudflare.com" /tmp/cloudflared.log | tail -3
# 발급 hostname (매 가동마다 변경): https://<random-name>.trycloudflare.com
```

#### 7-B. Cloudflare Pages 환경변수 갱신

Cloudflare 대시보드 → Workers & Pages → seoul-citydata-platform → Settings → Variables and Secrets:

| 변수 | 값 |
|---|---|
| `NODE_VERSION` | `20.19.0` (정확한 patch 명시 의무, asdf preset SoT) |
| `NEXT_PUBLIC_API_BASE` | `<tunnel hostname>` (위 발급 hostname) |

#### 7-C. Retry deployment

Deployments → "..." → Retry deployment. 약 2~3분 빌드 + deploy.

#### 7-D. caffeinate (sleep 방지) 의무

```bash
nohup caffeinate -i -t 90000 > /tmp/caffeinate.log 2>&1 & disown   # 25h
```

cloudflared / uvicorn / streaming 의 sleep 진입 방지. at job (예: 19:45 SLO 측정) fire 시각 + 1h 여유 의무 (PR δ §3 SoT).

#### 7-E. 브라우저 검증

```
https://seoul-citydata-platform.pages.dev/         # Leaflet 지도 + 마커
https://seoul-citydata-platform.pages.dev/chill/   # 가게 리스트
```

## 정지

```bash
# streaming + producer (graceful, hotfix #46 의 SIGTERM handler)
pkill -f "flink_jobs\.bronze_to_silver"
pkill -f "flink_jobs\.silver_to_gold"
pkill -f "flink_jobs\.cdc_to_dim_place"
pkill -f "producers\.hotspot_producer"
pkill -f "producers\.subway_producer"

# uvicorn
pkill -f "uvicorn"

# cloudflared (Pages 의 API 접근 차단됨)
pkill -f "cloudflared tunnel"

# caffeinate (자동 만료, 명시 kill 불요)
```

## 검증 — 데이터 신선도 + 작동

### 핵심 지표 — silver_arrival_ts (PR δ §6-5 SoT)

```bash
uv run python scripts/duckdb_check.py | tail -25
```

**streaming alive 의 정공 검증** = `silver_arrival_ts` 가 **현재 시간 - 5분 이내**. gold window_start 는 tumbling + watermark 의존이라 silver 보다 5~10분 뒤처지는 게 정상 (Day 4 archive §watermark SoT).

| 지표 | 정상 |
|---|---|
| bronze 누적 row | producer 5분 polling 마다 +3~9 row |
| silver_arrival_ts | 현재 시간 - 5분 이내 |
| gold window_start | silver 최신 window 보다 5~10분 뒤 |
| `/api/chill-open` count | 시각 따라 0~10 (영업 시간 + 한가 자치구 조합) |
| Cloudflare Pages chunk status | 모두 200 또는 304 (캐시됨) |

### 진단 명령

```bash
# 1. 운영 자원 alive
ps aux | grep -E "producers\.|flink_jobs\." | grep -v grep | wc -l   # 8 (wrapper 4 + child 4)
lsof -i :8000 | head -3                                              # uvicorn alive
ps aux | grep cloudflared | grep -v grep                             # tunnel alive
ps aux | grep caffeinate | grep -v grep                              # sleep 방지

# 2. API endpoint 작동
curl -sf http://localhost:8000/health                                # {"ok":true}
curl -sf http://localhost:8000/api/chill-open | python3 -m json.tool | head -20
curl -sf http://localhost:8000/api/hotspots/areas | python3 -m json.tool | head -20

# 3. tunnel 경유 API 작동
TUNNEL=$(grep "trycloudflare.com" /tmp/cloudflared.log | tail -1 | grep -oE "https://[a-z0-9-]+\.trycloudflare\.com")
curl -sf "$TUNNEL/api/chill-open" | head -c 200

# 4. streaming 신선도
uv run python scripts/duckdb_check.py | tail -25

# 5. cloudflared 재가동 후 새 hostname
grep "trycloudflare.com" /tmp/cloudflared.log | tail -3
# 재가동 시 새 URL → Cloudflare Pages 환경변수 + Retry deployment 의무
```

## 자주 발생 케이스

### chill_open_now mart 빌드 fail — `dbt.ref('dim_place')` Relation 처리

`dbt.ref('dim_place')` 의 Relation 객체를 f-string 으로 직접 박으면 데이터 dump 발생.

**해결** — Day 8 archive §1.2 SoT 의 `to_view` 패턴:

```python
dim_place_relation = dbt.ref("dim_place")
dim_place_relation.to_view("dim_place_view", replace=True)
# SQL 안에서 view name 으로 참조
sql = "... FROM dim_place_view ..."
```

본 패턴은 `dbt/seoul/models/marts/chill_open_now.py` 의 정착 사례. 향후 다른 dbt python model 에서 cross-ref 시 reuse 의무.

### Cloudflare Pages 의 chunk 500

브라우저 네트워크 탭에서 chunk JS/CSS 가 500 / `<script> 로드에 실패`:

**1차 진단** — 빌드 로그의 upload line:

```
✨ Success! Uploaded X files (Y already uploaded)
```

- Y > 0 → file dedup cache 발생 (이전 deploy 의 502 transient corrupt 보존)
- Y = 31 → 모든 file fresh upload

**해결 우선순위** (Day 8 archive §6 SoT):
1. **NODE_VERSION 정확한 patch 명시** (`20.19.0`, asdf preset SoT)
2. **NEXT_PUBLIC_API_BASE 환경변수 변경** (chunk hash 일부 영향)
3. **Pages project 재생성** (nuclear option, cache 완전 reset) ← 가장 확실

### asdf 의 NODE_VERSION fail

```
No preset version installed for command pnpm
asdf install nodejs 20.20.0
```

**해결** — 본 환경의 정확한 patch 명시:
- `20.19.0` (Node 20 LTS) ← 본 프로젝트 사용
- `22.16.0` (latest)
- 다른 버전은 본 환경 preset SoT (Day 8 archive §4 SoT)

### cloudflared 재가동 후 새 hostname

```bash
# 1. 이전 cloudflared kill (필요 시)
pkill -f "cloudflared tunnel"

# 2. 재가동
nohup cloudflared tunnel --url http://localhost:8000 > /tmp/cloudflared.log 2>&1 & disown

# 3. 새 hostname 추출
sleep 8
NEW_TUNNEL=$(grep "trycloudflare.com" /tmp/cloudflared.log | tail -1 | grep -oE "https://[a-z0-9-]+\.trycloudflare\.com")
echo "$NEW_TUNNEL"

# 4. Cloudflare Pages 대시보드 → Variables and Secrets → NEXT_PUBLIC_API_BASE 갱신
# 5. Deployments → Retry deployment
```

**자동화 불가** — 매 재가동 시 의무 사항. Phase 1B / 2 에서 영구 hostname (DuckDNS 또는 도메인) 진행 의무.

### streaming silent exit (hotfix #46 후)

hotfix PR #46 의 long-running default 후에는 `SMOKE_RUN_SECONDS=600` 자연 종료 사라짐. 그러나 다른 silent exit 가능:

- ClassLoader LinkageError (Day 4 archive SoT) — bronze_to_silver 의 `classloader.parent-first-patterns.additional` 누락 시
- uv extra mutual exclusive (PR δ §6-2 SoT) — review subagent 가 `uv sync --extra dev` 실행 후 pyflink 사라짐

**검증** (PR δ §6-5 + Day 4 archive SoT):

```bash
# 1. ps
ps aux | grep -E "flink_jobs\." | grep -v grep | wc -l   # 4 (wrapper 2 + child 2)

# 2. silver_arrival_ts
uv run python scripts/duckdb_check.py | tail -25

# 3. log (silent exit 일 때 stack trace 확인)
tail -50 /tmp/b2s.log
tail -50 /tmp/s2g.log
```

silent exit 시 재가동:
```bash
uv sync --extra flink   # pyflink 복원 의무
nohup uv run --extra flink python -m flink_jobs.bronze_to_silver > /tmp/b2s.log 2>&1 & disown
nohup uv run --extra flink python -m flink_jobs.silver_to_gold > /tmp/s2g.log 2>&1 & disown
```

## 메모리 / 비용 mitigation

| 항목 | 내용 |
|---|---|
| **streaming long-running default** (hotfix #46) | `FLINK_SMOKE_RUN_SECONDS=0` 의 1h heartbeat log 가 영구 누적. log 파일이 영구 증가 — `logrotate` 또는 `/tmp/*.log` 주기적 cleanup 의무 (Phase 1B 운영에서 자동화 검토) |
| **Cloudflare 비용** | Pages 무제한 + Tunnel 무료. 일 빌드 한도 500 (개인 사용 수준에서는 무관). 임시 hostname = $0 영구 |
| **임시 tunnel 비영구성** | cloudflared 재가동 시 새 hostname → 매 재가동 시 환경변수 + Retry deployment 의무. 자동화 불가. Phase 1B / 2 에서 영구 hostname (DuckDNS / 도메인) 진행 의무 |
| **caffeinate timer** | 25h (`-t 90000`) sleep 방지. at job fire 시각 + 1h 여유 의무 (PR δ §3 SoT). 만료 시 재가동 의무 |
| **Pages project 재생성** | nuclear option (Day 8 archive §6 SoT). 환경변수 + Build configuration + GitHub OAuth 재입력 의무 |

## 환경 편차 / 주의 사항

### Cloudflare Pages 의 asdf default

본 환경의 asdf preset:
- `14.21.3` / `16.20.2` (EOL)
- `18.17.1` (구 LTS, default)
- `20.19.0` (현재 LTS, **본 프로젝트 사용**)
- `22.16.0` (latest)

`NODE_VERSION=20` 입력 시 asdf 가 `20.20.0` (latest 20.x) 으로 자동 resolve → preset 에 없으면 fail. 정확한 patch 명시 의무 (Day 8 archive §4 SoT).

### `NEXT_PUBLIC_API_BASE` build-time inline

Next.js 14 static export 의 env injection — chunk 안에 build-time inline. dev fallback (`http://localhost:8000`) 이 prod build 에 그대로 남으면 mixed content 차단. env 명시 의무 (Day 7 PR γ §4 SoT).

### `pos-voltage-completing-compaq.trycloudflare.com`

본 시점 (2026-05-12 자정 무렵) 의 cloudflared 임시 hostname. cloudflared 재가동 시 새 URL 발급 — 본 runbook 의 hostname 은 변경 가능, 매 가동 시 `grep trycloudflare.com /tmp/cloudflared.log` 로 확인 의무.

### Pages project 의 영구 설정

Day 8 archive §6 SoT 의 Pages project 재생성 후의 현재 설정:
- Project name: `seoul-citydata-platform`
- Production branch: `main`
- Build command: `cd web && pnpm install --frozen-lockfile && pnpm build`
- Build output directory: `/web/out`
- Root directory: `/`
- Environment variables: `NODE_VERSION=20.19.0` + `NEXT_PUBLIC_API_BASE=<tunnel>`

## 관련 문서

- archive: 본 runbook 의 학습 자산 SoT 2건 (위 frontmatter link)
- Day 7 runbook: [`day7_deploy.md`](./day7_deploy.md) — Cloudflare 자동화 docs (Pages + Tunnel + ngrok fallback)
- Day 7 PR γ archive: [`../portfolio/troubleshooting/2026-05-11-day-7-nextjs-cloudflare-deploy.md`](../portfolio/troubleshooting/2026-05-11-day-7-nextjs-cloudflare-deploy.md)
- Day 7 PR δ archive: [`../portfolio/troubleshooting/2026-05-11-day-7-streaming-smoke-timeout-restart.md`](../portfolio/troubleshooting/2026-05-11-day-7-streaming-smoke-timeout-restart.md)
- Cloudflare 자동화 docs: [`../../infra/cloudflare/README.md`](../../infra/cloudflare/README.md)
- Plan: `docs/superpowers/plans/phase-1a-week-2.md` Day 8 Task 8.1~8.3 (line 1567~2037)
- Spec: `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` §6-1 Day 8 / §6 사용자 화면 #2
