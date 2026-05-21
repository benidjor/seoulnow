# Phase 1B Day 11 Prep — Cloudflare 계정/Pages/API 토큰 4 이슈

**발생일**: 2026-05-21 (Phase 1B Day 11 prep)
**관련 runbook**: [`02-cloudflare-account-and-api-token.md`](../../runbook/phase-1b-prep/02-cloudflare-account-and-api-token.md), [`03-cloudflare-pages-monorepo.md`](../../runbook/phase-1b-prep/03-cloudflare-pages-monorepo.md)
**관련 spec**: `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` §4
**관련 commits**: 본 prep 는 인프라 셋업만 (코드 commit 0), 단 frontend/index.html placeholder 1 commit

## 요약

Cloudflare 의 신규 unified UI 및 monorepo 구조로 인한 4개 이슈. 모두 해결책 발견 후 prep 진행 정상 재개.

| # | 이슈 | 진단 시간 | 해결 |
|---|---|---|---|
| 1 | Pages 가 Python `uv sync` 자동 실행해 빌드 실패 | ~20분 | `frontend/` 서브디렉토리 + Pages Root directory 설정 |
| 2 | Cloudflare 신규 unified create flow 가 Pages 가 아닌 Worker 생성 | ~15분 | 직접 URL `pages/new` 진입 또는 Pages tab 강제 클릭 |
| 3 | API 토큰 DNS 권한이 Account scope 에 없음 | ~5분 | Type 을 `Account` → `Zone` 으로 변경 |
| 4 | `wrangler whoami` grep 매치 실패 (박스 문자) | ~3분 | 검증 test 의 grep 패턴이 잘못, 실제 wrangler 정상 |

---

## Issue 1 — Pages 가 Python `uv sync` 를 자동 실행해 빌드 실패

### 증상

GitHub repo 를 Cloudflare Pages 에 연결한 직후 첫 자동 빌드 실패:

```
2026-05-20T10:21:46.261Z    Installing project dependencies: uv sync
2026-05-20T10:21:46.851Z    Using CPython 3.13.3 interpreter at: /opt/buildhome/.asdf/installs/python/3.13.3/bin/python3
2026-05-20T10:21:46.851Z    Creating virtual environment at: .venv
2026-05-20T10:21:47.150Z    Downloading confluent-kafka (3.8MiB)
2026-05-20T10:21:48.013Z    Building pyarrow==11.0.0
...
2026-05-20T10:21:51.845Z    × Failed to build `pyarrow==11.0.0`
2026-05-20T10:21:51.846Z    ModuleNotFoundError: No module named 'pkg_resources'
...
2026-05-20T10:21:51.883Z    Failed: error occurred while installing tools or dependencies
```

### 원인

본 repo 는 데이터 파이프라인 (Python) + frontend (Node.js) **monorepo**:
- repo root: `pyproject.toml` (PyFlink / Spark / dbt / Airflow 등 Python 의존성, ~137 packages)
- `frontend/` 서브디렉토리: Next.js + Pages Functions (TypeScript)

Cloudflare Pages 빌드 환경 초기화 단계 (build command 실행 **이전**) 가 자동으로 `pyproject.toml` 감지 → `uv sync` 실행. 그런데 `pyarrow==11.0.0` 이 Python 3.13 + ARM 환경에서 `pkg_resources` 의존성 누락으로 빌드 실패.

핵심 진단:
- Cloudflare Pages 가 monorepo 인지 모르고 root 의 모든 의존성 install 시도
- Build command 를 no-op 으로 바꿔도 못 고침 (의존성 install 이 build command 이전 단계)
- 단순한 Python deps 문제가 아니라 Pages 의 **빌드 환경 초기화 정책 자체**가 monorepo 와 불일치

### 해결

**Pages Root directory 를 `frontend/` 로 설정** → Pages 가 `frontend/` 디렉토리 안에서만 동작 → `pyproject.toml` 감지 자체가 안 됨.

#### Step 1. 로컬에 `frontend/` placeholder 생성

```bash
cd /Users/aryijq/Documents/01_DE_project/seoul-citydata-platform

mkdir -p frontend
cat > frontend/index.html <<'EOF'
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8" />
  <title>seoulnow — Phase 1B WIP</title>
</head>
<body>
  <h1>seoulnow.live</h1>
  <p>Phase 1B Day 11 implementation 진행 중. Pages Functions Edge API + Next.js frontend 가 곧 박힙니다.</p>
</body>
</html>
EOF

git add frontend/index.html
git commit -m "chore(frontend): Pages root directory 분리용 placeholder 추가"
git push origin main
```

#### Step 2. Pages 프로젝트 설정 변경

`Workers & Pages` → `seoulnow` 프로젝트 → `Settings` → `Build` → `Build configurations` → `Edit`:

| 항목 | 값 |
|---|---|
| Framework preset | `None` |
| Build command | (비워둠) |
| Build output directory | `.` (점 1개) |
| **Root directory (advanced)** | **`frontend`** |

#### Step 3. 재배포

`Deployments` → 최근 실패 빌드의 `...` → `Retry deployment`.

### 검증

새 빌드 로그가 다음과 같이 정상:

```
Initializing build environment...
Cloning repository...
Detected the following tools from environment: (empty)
No build command specified. Skipping build step.
Validating asset output directory
Uploading...
Success: Deployment complete
```

`uv sync` / `pyarrow` 라인이 안 나오면 정상 (Root directory=`frontend` 가 Python 감지 회피).

```bash
curl -s https://seoulnow.pages.dev/ | grep -q "Phase 1B WIP" && echo "✓ Pages 정상" || echo "✗"
```

### 회피 가능 여부

근본적으로 **monorepo + Cloudflare Pages = Root directory 분리 필수**. 단일 product repo 라면 발생 X.

### 후속 작업 (Day 11)

Day 11 implementation 시점에 `frontend/` 가 Next.js 구조로 확장 (`frontend/package.json`, `frontend/pages/`, `frontend/functions/v1/events.ts`). Pages preset 도 `None` → `Next.js` 로 변경 예정.

---

## Issue 2 — Cloudflare 신규 unified create flow 가 Pages 가 아닌 Worker 생성

### 증상

`Workers & Pages` → `Create application` → GitHub 연동으로 `benidjor/seoulnow` 선택 후 생성된 프로젝트가 **Worker 로 분류됨**. Build 설정 화면이 Pages 의 익숙한 UI (Framework preset / Build output directory) 가 아니라 Worker UI (`Deploy command: npx wrangler deploy` / `Non-production branch deploy command`) 로 표시.

진단 시그널 4가지:
- 좌측 사이드바 Recents 의 프로젝트 라벨 = `Workers` (Pages 가 아님)
- Build 패널의 `Deploy command: npx wrangler deploy` (Pages 에는 없는 필드)
- `Non-production branch deploy command: npx wrangler versions upload` (Workers Versions API 전용)
- "Build output directory" 필드 부재

### 원인

Cloudflare 가 최근 (2025 후반~2026) `Create application` 진입 시 default 를 **Workers (Workers Static Assets)** 로 설정. Git 연동하면 자동으로 Worker 프로젝트 생성됨. 이는 Cloudflare 의 product 통합 정책 (Pages → Workers 점진 통합) 의 결과.

그러나 본 프로젝트 spec (day11_prep.md / Phase 1B plan) 은 **Pages 프로젝트** 기준:
- `wrangler pages secret put` (Pages 전용 명령)
- `functions/v1/events.ts` (Pages Functions 패턴, Worker Static Assets 와 다름)

Worker 위에 그대로 진행하면 모든 명령 / 패턴이 깨짐.

### 해결

#### Step 1. Worker 삭제

`Workers & Pages` → `seoulnow` Worker 진입 → `Settings` → 스크롤 최하단 → `Delete` 또는 `Permanently delete all files, configurations, version...` → 프로젝트명 입력 confirm → 삭제.

#### Step 2. Pages 프로젝트로 재생성 — Pages tab 강제

**옵션 A (확실)**: 직접 URL 진입

```
https://dash.cloudflare.com/<account-id>/pages/new
```

예: `https://dash.cloudflare.com/<CLOUDFLARE_ACCOUNT_ID>/pages/new`

**옵션 B**: `Workers & Pages` → `Create application` → 상단 `Pages` tab 명시적 클릭 (default = Workers tab)

#### Step 3. GitHub 연동 + Pages 설정

Issue 1 의 Step 2 와 동일 (Project name=`seoulnow` / Production branch=`main` / Framework preset=`None` / Root directory=`frontend`).

### 검증

생성 직후 프로젝트가 Pages 로 분류됨:
- 좌측 사이드바 라벨 = `Pages`
- Build 패널에 `Build output directory`, `Framework preset` 필드 보임 (Worker 의 Deploy command 없음)
- dev URL = `seoulnow.pages.dev` (`*.workers.dev` 가 아님)

```bash
wrangler pages project list 2>&1 | grep seoulnow
# expected: seoulnow 프로젝트 행 (Pages project type)
```

### 회피 가능 여부

미래에 Cloudflare 가 Pages 를 완전 deprecate 하면 Workers Static Assets 로 마이그레이션 의무. 그 시점에 spec/plan 전면 수정 필요. 본 prep 시점 (2026-05) 은 Pages 가 여전히 first-class 지원이라 Pages 채택 정당함.

---

## Issue 3 — API 토큰 DNS 권한이 Account scope 에 없음

### 증상

API 토큰 Custom Token 생성 화면에서 4번째 권한 (DNS) 추가 시:

- Type 드롭다운: `Account` 기본 선택
- Resource 검색창에 `DNS` 입력 → 결과 리스트:
  - `DNS Firewall`
  - `DNS Settings`
  - `DNS Views`
- **단순 `DNS` 항목 없음**

위 3개 중 어느 것을 골라도 `cloudflared tunnel route dns` 명령이 CNAME 추가 시 권한 오류 발생.

### 원인

DNS 레코드 CRUD 권한은 **zone 에 종속된 자원** (`seoulnow.live` 의 DNS records). 따라서 Cloudflare 의 권한 scope 분류:

| Scope | DNS 관련 권한 | 본 프로젝트 필요? |
|---|---|---|
| **Zone** | `DNS` (DNS record CRUD) | ✅ 필요 (Tunnel route dns CNAME 추가) |
| Account | `DNS Firewall` (별도 product), `DNS Settings` (DNSSEC 등), `DNS Views` (Enterprise) | ❌ 모두 무관 |

Account scope 에서 `DNS` 검색 시 보이는 3개는 모두 zone 외부의 다른 product. 본 프로젝트가 필요한 zone DNS 권한은 Account scope 가 아니라 **Zone scope** 에 있음.

### 해결

4번째 권한 행의 Type 드롭다운을 **`Account` → `Zone`** 으로 변경 후 `DNS` 재검색:

```
Zone | DNS | Edit
```

`DNS` (suffix 없는 단일 항목) 가 결과 맨 위에 나옴. 설명 = "Grants edit access to DNS records".

추가로 `Zone Resources` 섹션 활성화:
- `Include` → `Specific zone` → **`seoulnow.live`** 선택

### 검증

토큰 생성 후 verify:

```bash
export CLOUDFLARE_API_TOKEN=$(grep '^CLOUDFLARE_API_TOKEN=' .env | cut -d= -f2-)
curl -s "https://api.cloudflare.com/client/v4/user/tokens/verify" \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" | jq .
# expected: "success": true, "status": "active"
```

이후 `cloudflared tunnel route dns seoulnow-receiver receiver.seoulnow.live` 가 CNAME 정상 추가:

```
INF Added CNAME receiver.seoulnow.live which will route to this tunnel
```

### 회피 가능 여부

Cloudflare 의 권한 분류가 직관적이지 않은 게 근본 원인 (`DNS` 가 Zone scope, `DNS Settings` 가 Account scope — 명명이 헷갈림). 첫 번째 API 토큰 생성 시 누구나 만나는 함정. 향후 Cloudflare 가 UI 개선해 `Zone:DNS` 로 prefix 명시하면 해소될 가능성.

---

## Issue 4 — `wrangler whoami` grep 매치 실패 (박스 문자)

### 증상

day11_prep.md §5 통합 체크리스트의 §1-login 검증:

```bash
wrangler whoami 2>&1 | grep -q "@" && echo "§1-login OK" || echo "§1-login FAIL"
```

→ `§1-login FAIL`

하지만 `wrangler whoami` 를 직접 실행하면 출력 정상:

```
⛅️ wrangler 4.93.0
───────────────────
Getting User settings...
👋 You are logged in with an User API Token. Unable to retrieve email for this user. ...
┌──────────────────────────────┬──────────────────────────────────┐
│ Account Name                 │ Account ID                       │
├──────────────────────────────┼──────────────────────────────────┤
│ <YOUR_EMAIL>'s Account │ <CLOUDFLARE_ACCOUNT_ID> │
└──────────────────────────────┴──────────────────────────────────┘
```

Account ID `<CLOUDFLARE_ACCOUNT_ID>` 가 박스 안에 표시되는데 grep 매치 안 됨.

### 원인

두 가지가 겹침:

1. **`CLOUDFLARE_API_TOKEN` env 가 설정된 상태에서 wrangler 가 OAuth 토큰 대신 API 토큰 사용** — API 토큰엔 `User->User Details->Read` 권한이 없어서 email (`@` 문자 포함) 이 안 표시됨. 따라서 `grep "@"` 매치 안 됨
2. **출력의 박스 문자 (`│`, `─` 등) 가 grep 결과를 가독성 떨어트림** — grep -q 자체는 정상 작동하지만 다른 시그널을 찾아야 함

### 해결

#### 방법 A (권장): Account ID 로 검증

```bash
wrangler whoami 2>&1 | grep -q "<CLOUDFLARE_ACCOUNT_ID>" && echo "§1-login OK" || echo "§1-login FAIL"
```

#### 방법 B: OAuth 토큰으로 전환 후 검증

```bash
unset CLOUDFLARE_API_TOKEN
wrangler whoami 2>&1 | grep -q "@" && echo "§1-login OK (OAuth)" || echo "§1-login FAIL"

# 재박기
export CLOUDFLARE_API_TOKEN=$(grep '^CLOUDFLARE_API_TOKEN=' .env | cut -d= -f2-)
```

#### 방법 C: 어떤 토큰이든 정상 응답 확인

```bash
wrangler whoami 2>&1 | grep -qE "(active|@|Account ID)" && echo "§1 OK"
```

### 검증

방법 A 가 가장 안정. day11_prep.md §5 의 grep 패턴을 향후 갱신 시 `"@"` → `"8a41b2ef..."` (또는 일반화 `Account ID`) 로 교체 권장.

### 회피 가능 여부

shell test script 의 grep 패턴 문제. 실제 wrangler 와 API 토큰 모두 정상이었음. 본질적인 셋업 이슈는 아니지만, day11_prep.md doc 의 검증 명령을 정정해야 향후 prep 시점에 같은 confusion 회피 가능.

---

## 통합 lesson learned

1. **Cloudflare 의 신규 unified UI 가 Pages → Workers 통합 방향이라 Pages 사용 시 명시적 진입 의무** (Issue 2)
2. **monorepo 는 Pages Root directory 분리 필수** (Issue 1) — 본 패턴은 향후 Cloudflare docs 에 추가됐으면 함
3. **DNS 권한은 Zone scope** (Issue 3) — Cloudflare 의 권한 명명이 비직관적인 함정
4. **shell test script 의 grep 패턴은 출력 형식 변경에 fragile** (Issue 4) — Account ID 같은 안정적 토큰으로 매치하는 게 좋음

## 관련 문서

- runbook: [`02-cloudflare-account-and-api-token.md`](../../runbook/phase-1b-prep/02-cloudflare-account-and-api-token.md), [`03-cloudflare-pages-monorepo.md`](../../runbook/phase-1b-prep/03-cloudflare-pages-monorepo.md)
- 다른 troubleshooting: [`2026-05-21-day-11-prep-oracle-cloud-issues.md`](./2026-05-21-day-11-prep-oracle-cloud-issues.md), [`2026-05-21-day-11-prep-tunnel-and-shell-issues.md`](./2026-05-21-day-11-prep-tunnel-and-shell-issues.md)
