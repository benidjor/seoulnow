# 02 — Cloudflare 계정 + Wrangler + API 토큰

> **목표**: Cloudflare 의 5 product (Workers/Pages/D1/Zero Trust) 활성화 + Wrangler CLI 설치 + Pages/Workers/D1/DNS/Tunnel 5권한 API 토큰 발급
> **결과**: Account ID `<CLOUDFLARE_ACCOUNT_ID>`, Zero Trust team `benidjor`, Workers subdomain `benidjor.workers.dev`, API 토큰 `.env` 저장
> **소요**: 30분
> **전제**: [`01-domain-and-dns.md`](./01-domain-and-dns.md) 완료 (`seoulnow.live` zone Active)

## 1. 계정 활성화 (Workers/Pages/D1/Zero Trust 4개 product)

각 product 는 첫 진입 시 1회 클릭으로 자동 활성화. 별도 결제 / 신청 필요 X (모두 Free Tier 자동 부여).

### 1-1. Workers & Pages 활성화

1. Cloudflare dashboard 좌측 사이드바 → **`Workers & Pages`** 클릭 (또는 `Compute` → `Workers & Pages`)
2. 첫 진입 시 **Worker 서브도메인 설정 화면** 표시:
   - 입력: `benidjor` (영문 소문자 / 숫자 / 하이픈만)
   - 향후 모든 Worker 의 기본 URL 에 박힘 (`*.benidjor.workers.dev`)
   - 변경이 매우 번거로움 — 신중히 선택, GitHub 핸들 / 이메일 핸들 추천
3. `Set up` → 활성화 완료

### 1-2. D1 활성화

1. 좌측 사이드바 → **`Storage & Databases`** → **`D1 SQL Database`** 클릭
2. 빈 리스트 페이지 (`Create database` 버튼만 보임) 도달 → **활성화 완료** (Create 누를 필요 X — Day 12 작업)

### 1-3. Zero Trust 활성화 (Tunnel 사용 위해 필수)

1. 좌측 사이드바 → **`Zero Trust`** (또는 `Protect & Connect` 카테고리 아래) 클릭
2. 첫 진입 시 `Welcome to Cloudflare Zero Trust` 환영 페이지 → 우상단 **`Get started`** 클릭
3. **Team name (slug) 입력**:
   - 권장: **`benidjor`** (Workers subdomain 과 통일)
   - 영문 소문자 / 숫자 / 하이픈만
   - 향후 `benidjor.cloudflareaccess.com` URL 의 일부로 영구 박힘 (변경 어려움)
4. Plan 선택 → **`Free`** (50 user 까지 무료)
5. 결제수단 등록 요구 시 카드 등록 (Free 라 과금 0원, abuse 방지 정책)
6. Authentication method → `One-time PIN` (가장 간단, Phase 1B 에선 Tunnel ingress only 용도라 사용자 로그인 X)
7. `Save` → Zero Trust 대시보드 진입 시 좌측 메뉴에 `Networks` / `Access` / `Gateway` 등 보이면 성공

## 2. Wrangler CLI 설치 + login (로컬 macOS)

### 2-1. Node 20+ 확인

```bash
node --version
# expected: v20.x 또는 v22.x 이상 (v18 이하면 brew install node@20)
```

### 2-2. Wrangler 4.x 글로벌 설치

```bash
npm install -g wrangler@4

# 검증
wrangler --version
# expected: ⛅️ wrangler 4.93.0
```

`EACCES` 권한 에러 시 npm prefix 변경:

```bash
mkdir -p ~/.npm-global
npm config set prefix ~/.npm-global
echo 'export PATH=~/.npm-global/bin:$PATH' >> ~/.zshrc
source ~/.zshrc
npm install -g wrangler@4
```

### 2-3. OAuth 로그인

```bash
wrangler login
```

자동으로 브라우저 탭 열림 → Cloudflare 로그인 → **`Authorize Wrangler`** 클릭 → 터미널에 `Successfully logged in.` 메시지.

## 3. Custom API 토큰 발급 (5권한)

OAuth 토큰은 인터랙티브 CLI 용. **`.env` 저장 + 향후 CI / 비대화형 스크립트 용**으로 별도 API 토큰 발급.

### 3-1. API Tokens 페이지 진입

<https://dash.cloudflare.com/profile/api-tokens> 또는 우상단 프로필 아이콘 → `My Profile` → `API Tokens` 메뉴.

### 3-2. Create Custom Token

1. **`Create Token`** 클릭
2. 여러 템플릿 중 **`Create Custom Token`** 의 **`Get started`** 클릭

### 3-3. 토큰 설정 입력

| 항목 | 값 |
|---|---|
| Token name | `seoulnow-phase1b-local` |

**Permissions 5개** (`+ Add more` 로 추가):

| # | Type | Resource | Access |
|---|---|---|---|
| 1 | **Account** | Cloudflare Pages | Edit |
| 2 | **Account** | Workers Scripts | Edit |
| 3 | **Account** | D1 | Edit |
| 4 | **Zone** ★ | **DNS** (suffix 없는 단일 항목) | Edit |
| 5 | **Account** | Cloudflare Tunnel | Edit |

⚠️ **4번 DNS 는 Type 을 `Zone` 으로 설정해야** DNS 단일 항목이 보임. `Account` 으로 두면 DNS Firewall / DNS Settings / DNS Views 만 보임 (모두 본 프로젝트 무관). 트러블슈팅 → [`2026-05-21-day-11-prep-cloudflare-account-issues.md#issue-3`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-cloudflare-account-issues.md#issue-3--api-토큰-dns-권한이-account-scope-에-없음)

**Account Resources**:
- `Include` → `Specific account` → `<YOUR_EMAIL>'s Account` 선택

**Zone Resources** (Zone · DNS · Edit 가 있으므로 필수):
- `Include` → `Specific zone` → `seoulnow.live` 선택

**Client IP Address Filtering / TTL**: 비워두기 (defaults)

### 3-4. Continue to summary → Create Token

요약 화면에서 5 권한 모두 보이는지 검증 → `Create Token`.

### 3-5. 토큰 값 ⚠️ 즉시 복사

화면에 단 1회만 표시. 페이지 새로고침 시 영구 못 봄. 즉시 메모장 / 1Password 임시 보관 (대화창 / commit 절대 X).

### 3-6. 로컬 `.env` 저장

```bash
cd /Users/aryijq/Documents/01_DE_project/seoul-citydata-platform

# .gitignore 에 .env 차단 확인
grep -E '^\.env$' .gitignore && echo "OK: .env 차단됨" || echo "WARNING: .gitignore 에 .env 추가 필요"

# 토큰 + Account ID 저장
echo "CLOUDFLARE_API_TOKEN=<복사한 토큰 값>" >> .env
echo "CLOUDFLARE_ACCOUNT_ID=<CLOUDFLARE_ACCOUNT_ID>" >> .env

# 권한 좁힘
chmod 600 .env
```

Account ID 는 Workers & Pages 페이지의 `Account Details` 섹션 또는 우측 사이드바에서 확인 가능.

## 4. 검증 명령

### 4-1. Wrangler login 검증

```bash
wrangler whoami
```

예상 출력 (API 토큰 사용 시):

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

⚠️ `Unable to retrieve email` 메시지는 정상 (API 토큰에 `User->User Details->Read` 권한 미포함). OAuth 로그인으로도 검증하려면:

```bash
unset CLOUDFLARE_API_TOKEN
wrangler whoami
# expected: 👋 You are logged in with an OAuth Token, associated with the email <YOUR_EMAIL>.
# 다시 박기:
export CLOUDFLARE_API_TOKEN=$(grep '^CLOUDFLARE_API_TOKEN=' .env | cut -d= -f2-)
```

### 4-2. API 토큰 직접 검증 (curl)

```bash
export CLOUDFLARE_API_TOKEN=$(grep '^CLOUDFLARE_API_TOKEN=' .env | cut -d= -f2-)

curl -s -X GET "https://api.cloudflare.com/client/v4/user/tokens/verify" \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" | jq .
```

예상 출력:

```json
{
  "result": {
    "id": "<token-id>",
    "status": "active"
  },
  "success": true,
  "errors": [],
  "messages": [
    {
      "code": 10000,
      "message": "This API Token is valid and active",
      "type": null
    }
  ]
}
```

`"success": true` + `"status": "active"` 모두 확인되면 API 토큰 정상.

### 4-3. `.env` 의 2 entry 존재 검증

```bash
grep -c '^CLOUDFLARE_API_TOKEN=' .env   # → 1
grep -c '^CLOUDFLARE_ACCOUNT_ID=' .env  # → 1
git ls-files .env                       # → 빈 출력 (추적 안 됨)
```

### 4-4. Zero Trust team 활성 검증

```bash
curl -sI https://benidjor.cloudflareaccess.com/cdn-cgi/access/login | head -5
# expected: HTTP/2 404 + cf-version + server: cloudflare
# 404 자체는 정상 (Access App 미설정), 응답 헤더의 cloudflare 표시가 team 존재 증거
```

## 5. 보안 권장 — 토큰 노출 시 회전

API 토큰이 대화창 / commit / log 등 어디든 노출되면 **즉시 회전**:

1. <https://dash.cloudflare.com/profile/api-tokens> → 해당 토큰 `...` 메뉴 → **`Roll`**
2. 새 토큰 즉시 복사
3. 로컬 `.env` 의 `CLOUDFLARE_API_TOKEN=` 값 교체
4. `curl ... user/tokens/verify` 재검증

본 prep 세션에서는 토큰이 대화창에 노출됐으나 권한이 좁고 (Pages/Workers/D1/Tunnel + DNS Edit, Zone specific) blast radius 가 작아 사용자 판단으로 회전 skip.

## 6. 관련 troubleshooting

[`2026-05-21-day-11-prep-cloudflare-account-issues.md`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-cloudflare-account-issues.md):
- Issue 3: API 토큰 DNS 권한이 Account scope 에 없음 (`Zone` 으로 변경 필요)
- Issue 4: `wrangler whoami` grep 매치 실패 (출력 안의 박스 문자)

## 7. 다음 단계

→ [`03-cloudflare-pages-monorepo.md`](./03-cloudflare-pages-monorepo.md) — Pages 프로젝트 생성 (monorepo `frontend/` Root directory)
