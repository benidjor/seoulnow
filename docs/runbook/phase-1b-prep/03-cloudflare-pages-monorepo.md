# 03 — Cloudflare Pages (monorepo with `frontend/`)

> **목표**: Cloudflare Pages 프로젝트 `seoulnow` 생성 + monorepo 패턴 (`frontend/` 서브디렉토리) 으로 Python uv sync 자동 빌드 회피
> **결과**: `seoulnow.pages.dev` placeholder HTML 표시, Pages → GitHub `benidjor/seoulnow` 연동, Root directory=`frontend`
> **소요**: 15분 (단, 처음 Worker 로 잘못 만들면 재생성 10분 추가)
> **전제**: [`02-cloudflare-account-and-api-token.md`](./02-cloudflare-account-and-api-token.md) 완료 + GitHub repo `seoulnow` 존재

## 1. 사전 결정 — GitHub repo 이름 결정 / rename

본 프로젝트 repo 가 `seoul-citydata-platform` 였다면 도메인 `seoulnow.live` 와 일관성을 위해 **`seoulnow` 로 rename** 권장 (Pages 프로젝트 이름 = `seoulnow`, dev URL `seoulnow.pages.dev`, Custom domain `seoulnow.live` 까지 4축 통일).

### 1-1. GitHub repo rename

1. <https://github.com/benidjor/seoul-citydata-platform> 진입
2. `Settings` → `General` → `Repository name` → **`seoulnow`** → `Rename`
3. GitHub 자동 redirect (old URL → new URL) 박힘 — 깨지는 link 없음

### 1-2. 로컬 git remote 갱신

```bash
cd /Users/aryijq/Documents/01_DE_project/seoul-citydata-platform

git remote -v   # origin URL 확인
git remote set-url origin https://github.com/benidjor/seoulnow.git
git remote -v   # 변경 반영

git fetch origin   # 에러 없으면 OK
```

### 1-3. 로컬 working directory 는 rename 하지 않음

**`/Users/aryijq/Documents/01_DE_project/seoul-citydata-platform/` 그대로 유지** — Claude Code 메모리 경로 (`~/.claude/projects/-Users-aryijq-Documents-01-DE-project-seoul-citydata-platform/`) 가 이 경로 기반이라 변경 시 메모리 연속성 손실. GitHub repo 이름과 로컬 directory 이름이 달라도 git 동작 영향 X.

## 2. Pages 프로젝트 생성 — Pages tab 강제 사용

⚠️ Cloudflare 의 신규 unified `Create application` 흐름이 default 로 **Worker** 를 생성합니다. Pages 프로젝트로 만들려면 명시적으로 Pages tab 선택 또는 직접 URL 진입.

### 2-1. Pages 프로젝트 신규 생성

**옵션 A (권장): 직접 URL 진입** — 가장 확실

```
https://dash.cloudflare.com/<account-id>/pages/new
```

예: <https://dash.cloudflare.com/<CLOUDFLARE_ACCOUNT_ID>/pages/new>

**옵션 B: Workers & Pages → Create application → 상단 `Pages` tab 강제 클릭**

### 2-2. GitHub 연동

1. `Connect to Git` (또는 `Import an existing Git repository`) 클릭
2. GitHub 권한 부여 (첫 사용 시) → 본인 계정의 repo 목록 표시
3. **`benidjor/seoulnow`** 선택 → `Begin setup`

### 2-3. Build & Deploy 설정 ★ 핵심

| 필드 | 입력값 |
|---|---|
| Project name | **`seoulnow`** (소문자, `*.pages.dev` URL prefix) |
| Production branch | `main` |
| Framework preset | **`None`** (Day 11 implementation 시점에 `Next.js` 로 변경) |
| Build command | **(비워둠)** |
| Build output directory | `.` (점 한 개) 또는 비워둠 |
| **Root directory (advanced)** | **`frontend`** ★ (이게 핵심 — 다음 §3 참조) |

`Save and Deploy` 클릭.

## 3. monorepo 패턴 — Root directory=`frontend/` 의 필요성

### 3-1. 왜 `frontend/` 서브디렉토리가 필요한가

본 repo 는 **데이터 파이프라인 + frontend 통합 monorepo** 구조:

- repo root: `pyproject.toml` (PyFlink / Spark / dbt / Airflow 등 Python 의존성)
- `frontend/`: Next.js + Pages Functions (TypeScript)

Cloudflare Pages 의 빌드 환경 초기화 단계는 자동으로:
- `pyproject.toml` 감지 → `uv sync` (Python 의존성 install) 실행
- `package.json` 감지 → `npm install` 실행

본 repo 루트에 `pyproject.toml` 이 있어서 Pages 가 `uv sync` 를 자동 실행 → `pyarrow==11.0.0` 이 Python 3.13 + ARM 환경에서 `pkg_resources` 의존성 누락으로 빌드 실패. 트러블슈팅 → [`2026-05-21-day-11-prep-cloudflare-account-issues.md#issue-1`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-cloudflare-account-issues.md#issue-1--pages-가-pythonp-uv-sync-를-자동-실행해-빌드-실패)

해결: Pages 의 **Root directory** 를 `frontend/` 로 설정하면 빌드 환경이 `frontend/` 안에서만 동작 → `pyproject.toml` 감지 안 함 → Python install 자체가 trigger 안 됨.

### 3-2. `frontend/` placeholder 생성 (Day 11 implementation 전에는 placeholder 만)

Day 11 implementation 시점에 본격 Next.js 코드가 들어오지만, prep 단계에서는 빌드 success 만 필요하므로 minimal placeholder 충분.

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

# .gitignore 가 frontend/ 차단 안 하는지 확인
grep -E '^frontend/?$' .gitignore && echo "WARNING: frontend 가 .gitignore 에 있음" || echo "OK"

# commit + push
git add frontend/index.html
git commit -m "chore(frontend): Pages root directory 분리용 placeholder 추가

Cloudflare Pages 가 repo 루트의 pyproject.toml 을 감지하고 데이터 파이프라인
의존성 (pyarrow 등) 을 uv sync 로 설치 시도하다 실패. Pages 의 Root directory 를
frontend/ 로 분리해 monorepo 패턴 정착.

Day 11 Task 11.1 implementation 시점에 본 디렉토리에 Next.js + Pages Functions
(functions/v1/events.ts) 가 추가됨.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"

git push origin main
```

### 3-3. heredoc 함정 주의

⚠️ 위 `cat > ... <<'EOF' ... EOF` heredoc 종료 마커 `EOF` 는 **줄 맨 앞 (column 0)** 에 와야 함. paste 시 들여쓰기 박히면 무한 대기. 트러블슈팅 → [`2026-05-21-day-11-prep-tunnel-and-shell-issues.md#issue-3`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-tunnel-and-shell-issues.md#issue-3--heredoc-eof-종료-마커-들여쓰기-함정)

## 4. Pages 빌드 결과 확인

`Deployments` 탭에서 자동 trigger 된 첫 빌드 (또는 `Retry deployment`):

```
Initializing build environment...
Cloning repository...
Detected the following tools from environment: (empty)
No build command specified. Skipping build step.
Validating asset output directory
Uploading...
Success: Deployment complete
Deployed to: https://<hash>.seoulnow.pages.dev
```

`uv sync` / `pyarrow` / `pyiceberg` 같은 라인이 **안 나와야 정상** (Root directory=`frontend` 덕분에 Python 감지 안 됨).

## 5. 검증 명령

### 5-1. placeholder 페이지 응답 확인 (로컬 macOS)

```bash
curl -s https://seoulnow.pages.dev/ | grep -q "Phase 1B WIP" && echo "✓ Pages 정상" || echo "✗ Pages placeholder 미배포"
```

브라우저로 <https://seoulnow.pages.dev> 접속 시 placeholder HTML (seoulnow.live h1 + Phase 1B WIP 문구) 표시.

### 5-2. HTTPS 헤더 확인

```bash
curl -sI https://seoulnow.pages.dev/ | head -5
# expected: HTTP/2 200 + server: cloudflare + cf-ray:
```

### 5-3. Pages 프로젝트 메타데이터 (wrangler)

```bash
cd /Users/aryijq/Documents/01_DE_project/seoul-citydata-platform
wrangler pages project list 2>&1 | grep seoulnow
# expected: seoulnow 프로젝트 행 표시 (Production branch=main, 최근 deploy 시각)
```

### 5-4. GitHub 연동 + 자동 배포 검증

main branch 에 임의 commit push → Cloudflare Pages 의 `Deployments` 탭에서 자동 빌드 trigger 확인.

## 6. (Day 11 implementation 시점 작업) Custom domain 추가

prep 단계에서는 **하지 않음**. Day 11 Task 11.1 진입 직전 1분 작업:

1. Cloudflare dashboard → `Workers & Pages` → `seoulnow` 프로젝트 진입
2. 상단 탭 **`Custom domains`** 클릭
3. **`Set up a domain`** → `seoulnow.live` 입력 (apex)
4. Cloudflare 가 zone 안에 CNAME 자동 추가 (Cloudflare Registrar / NS 가 이미 Cloudflare 라서 즉시 active)
5. 1~5분 후 `https://seoulnow.live/` 가 Pages placeholder 로 응답

## 7. (Day 11 implementation 시점 작업) Pages Secrets 등록

prep 단계에서는 **하지 않음**. Day 11 Task 11.1 implementation 시점:

```bash
cd /Users/aryijq/Documents/01_DE_project/seoul-citydata-platform

# RECEIVER_TOKEN secret (Pages Functions Edge API 에서 process.env 로 읽음)
grep '^RECEIVER_TOKEN=' .env | cut -d= -f2- | wrangler pages secret put RECEIVER_TOKEN --project-name=seoulnow

# ANON_UA_SALT secret (ua_hash SHA-256 salt)
grep '^ANON_UA_SALT=' .env | cut -d= -f2- | wrangler pages secret put ANON_UA_SALT --project-name=seoulnow

# 검증
wrangler pages secret list --project-name=seoulnow
# expected: RECEIVER_TOKEN, ANON_UA_SALT 2 entry 표시 (값은 안 보임)
```

## 8. 관련 troubleshooting

[`2026-05-21-day-11-prep-cloudflare-account-issues.md`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-cloudflare-account-issues.md):
- Issue 1: Pages 가 Python uv sync 자동 실행해 빌드 실패 (본 doc 의 monorepo 패턴 = 해결)
- Issue 2: Cloudflare 신규 unified create flow 가 Pages 아닌 Worker 생성 (본 doc 의 Pages tab 강제 = 해결)

## 9. 다음 단계

→ [`04-cloudflare-tunnel.md`](./04-cloudflare-tunnel.md) — cloudflared 설치 + Tunnel 생성 + DNS routing + systemd

(병렬로 진행 가능: [`05-oracle-cloud-vm.md`](./05-oracle-cloud-vm.md) — Oracle VM 셋업이 Tunnel 의 hosting 환경)
