# Day 11 사전 준비 — 외부 시스템 셋업 가이드 (4건)

> Phase 1B Day 11 본격 진입 이전, 사용자가 직접 셋업해야 하는 외부 시스템 4종.
> 본 doc 의 범위 = **셋업 절차 + 검증 명령만**. 코드 변경 / PR 은 Day 11 implementation 에서 별도 처리.
>
> **plan SoT**: `docs/superpowers/plans/phase-1b-week-3.md` Day 11 (Task 11.1 / 11.2 / 11.3)
> **progress SoT**: `~/.claude/projects/-Users-aryijq-Documents-01-DE-project-seoul-citydata-platform/memory/phase-1b-progress.md` §"다음 작업 §0"

---

## 0. 개요

| # | 항목 | 어디서 | 산출물 (어디에 저장) | 예상 소요 |
|---|---|---|---|---|
| 1 | Cloudflare 계정 + API 토큰 | dash.cloudflare.com | `CLOUDFLARE_API_TOKEN` (로컬 `.env`, 절대 git X) | 30분 |
| 2 | Bearer Token Secret | 로컬 생성 + Cloudflare Pages + Oracle VM `.env` | `RECEIVER_TOKEN` + `ANON_UA_SALT` | 15분 |
| 3 | Cloudflare Tunnel (Oracle Cloud → Cloudflare) | Oracle VM + Cloudflare dashboard | `~/.cloudflared/*.json` + DNS 라우트 | 60-90분 |
| 4 | VAPID 키 페어 (Web Push, Day 13 선행) | 로컬 1회 생성 | `VAPID_PUBLIC_KEY` + `VAPID_PRIVATE_KEY` | 10분 |

**의존 순서 (병렬 가능 항목 명시):**

```
(4. VAPID)              ← 독립, 언제든 가능
   ↓
(1. Cloudflare 계정/토큰) ← 도메인 활성화 / Wrangler 로그인 의무
   ↓
(2. Bearer Secret) → (3. Tunnel) ← 1번 의존, 2·3 은 병렬 가능
```

**전제 (Day 0 prep §17 SoT):**
- Oracle Cloud Always Free VM (ARM Ampere A1, ubuntu) 1대 가동 + ssh 접근 가능
- 도메인 1개 (Cloudflare Registrar 또는 외부 registrar + Cloudflare DNS) — Tunnel 라우팅에 필수. `*.pages.dev` 만으로는 Tunnel ingress 불가
- Node 20+ / npm 로컬 설치
- 본 프로젝트 repo 가 macOS 로컬에서 git working tree clean (`54328f1`)

---

## 1. Cloudflare 계정 + API 토큰

**왜 필요?** Cloudflare Pages Functions (Edge API, Task 11.1) + D1 (Day 12) + Workers Cron (Day 13) + Tunnel (본 doc §3) 4 product 전부 동일 계정에서 활성화. Wrangler CLI 로 deploy / secret 관리.

### 1.1. 무료 계정 가입 + 도메인 연결

1. <https://dash.cloudflare.com/sign-up> 에서 이메일 / 비밀번호 가입 (2FA 권장)
2. 좌측 사이드바 **Websites → Add a site** 에서 도메인 추가
   - Free plan 선택
   - registrar 의 NS 를 Cloudflare 가 제공하는 값으로 변경 (Cloudflare Registrar 이면 자동)
3. **Workers & Pages** 메뉴 진입 → 좌측 **D1 SQL Database**, **Pages**, **Workers** 메뉴 클릭 1회 (활성화)
4. 좌측 **Zero Trust** 메뉴 진입 → 팀 이름 (slug) 설정 (Tunnel 사용 위해 필수)

### 1.2. Wrangler CLI 설치 + 로그인 (로컬 macOS)

```bash
# 로컬에 Node 20+ 가 있어야 함
node --version   # v20.x 이상 확인
npm install -g wrangler@4    # 글로벌 설치 권장 (프로젝트 종속 안 함)

wrangler --version           # 4.x.x 출력 확인
wrangler login               # 브라우저 OAuth 창 열림 → 로그인 → "Allowed"
```

### 1.3. API 토큰 생성 (로컬 `.env` 용, 자동화 / CI 대비)

Dashboard → 우상단 프로필 아이콘 → **My Profile → API Tokens → Create Token**.

| 권한 | 범위 |
|---|---|
| Account · Cloudflare Pages · Edit | (해당 account) |
| Account · Workers Scripts · Edit | (해당 account) |
| Account · D1 · Edit | (해당 account) |
| Zone · DNS · Edit | (Tunnel 라우팅용, 해당 zone) |
| Account · Cloudflare Tunnel · Edit | (해당 account) |

생성 후 **Continue to summary → Create Token** → 한 번만 표시되는 토큰 값을 즉시 복사.

```bash
# 로컬 .env 에 저장 (.gitignore 의 .env 항목 이미 박혀 있음)
# 본 토큰은 절대 commit X — wrangler login 의 OAuth credential 과 별도
echo 'CLOUDFLARE_API_TOKEN=<paste-token-here>' >> .env
echo 'CLOUDFLARE_ACCOUNT_ID=<dashboard 우측 account id>' >> .env
```

### 1.4. 검증

```bash
# wrangler login 검증
wrangler whoami
# expected: 본인 이메일 + Account ID 표시

# API 토큰 검증 (curl 직접)
curl -s -X GET "https://api.cloudflare.com/client/v4/user/tokens/verify" \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" | jq .
# expected: "success": true + status "active"
```

---

## 2. Bearer Token Secret (`RECEIVER_TOKEN` + `ANON_UA_SALT`)

**왜 필요?** `user.events.v1` 발행 권한 — Cloudflare Pages Functions (Edge API) 만이 Oracle Cloud HTTP receiver 에 POST 가능해야 함. 토큰 불일치 = HTTP 401 (Task 11.2 pydantic 검증 SoT). `ANON_UA_SALT` 는 `ua_hash = SHA-256(ua + salt)` 의 salt — Web 환경 노출 X.

### 2.1. 토큰 2종 로컬 생성 (32 bytes 고엔트로피)

```bash
# RECEIVER_TOKEN — Edge API ↔ receiver Bearer
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
# 예시 출력: xy7Q8wJ-vB3...  (44자 base64url)

# ANON_UA_SALT — ua_hash salt (Pages Functions secret)
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
# 다른 값으로 생성 의무 (재사용 금지)
```

두 값 모두 안전한 장소 (예: 1Password, macOS Keychain, 또는 임시 메모) 에 보관. 이후 단계에서 (a) Cloudflare Pages secret (b) Oracle VM `.env` 양쪽에 동일 값 박힘.

### 2.2. Cloudflare Pages secret 등록 (Task 11.1 Edge API 가 읽음)

Day 11 implementation 시점에 Pages 프로젝트가 만들어진 후 등록 가능. 본 prep 단계에서는 **로컬 `.env` 에 임시 저장만**:

```bash
# 로컬 .env (Day 11 implementation 까지 임시 보관)
cat >> .env <<EOF

# Phase 1B Day 11 — Edge API ↔ receiver Bearer + ua_hash salt
RECEIVER_TOKEN=<위 §2.1 RECEIVER_TOKEN 값>
ANON_UA_SALT=<위 §2.1 ANON_UA_SALT 값>
EOF

# .env 가 .gitignore 에 있는지 재확인 (Phase 1A 부터 박혀 있어야 함)
grep -E '^\.env$' .gitignore || echo "WARNING: .gitignore 에 .env 미등록 — 즉시 추가 의무"
```

Day 11 Task 11.1 implementation 시점에 (Pages 프로젝트 생성 후):

```bash
# Pages Functions 가 process.env 로 읽을 수 있도록 secret 등록
# (실제 실행은 Day 11 implementation 시점)
echo $RECEIVER_TOKEN | wrangler pages secret put RECEIVER_TOKEN --project-name=seoul-citydata
echo $ANON_UA_SALT   | wrangler pages secret put ANON_UA_SALT   --project-name=seoul-citydata
```

### 2.3. Oracle VM `.env` 동기화 (Task 11.2 receiver 가 읽음)

Oracle Cloud VM 에 ssh 후:

```bash
# VM 안 본 프로젝트 clone 위치로 이동 (없으면 git clone 먼저)
cd ~/seoul-citydata-platform
cp .env.example .env 2>/dev/null  # 없으면 신규 생성

# Phase 1A 시점 entry 모두 유지하고 끝에 추가
cat >> .env <<EOF

# Phase 1B Day 11 — receiver Bearer
RECEIVER_TOKEN=<§2.1 RECEIVER_TOKEN 동일 값>
EOF

chmod 600 .env   # 권한 좁힘
```

### 2.4. 검증

```bash
# 로컬 .env 검증
grep -c '^RECEIVER_TOKEN=' .env       # → 1
grep -c '^ANON_UA_SALT='  .env        # → 1

# VM 측 검증 (ssh 후)
ssh ubuntu@<oracle-vm-ip> "grep -c '^RECEIVER_TOKEN=' ~/seoul-citydata-platform/.env"
# expected: 1

# 두 값 일치 확인 (양쪽 .env 의 hash 비교)
# 로컬:
md5 -q <(grep '^RECEIVER_TOKEN=' .env | cut -d= -f2-)
# VM:
ssh ubuntu@<oracle-vm-ip> "grep '^RECEIVER_TOKEN=' ~/seoul-citydata-platform/.env | cut -d= -f2- | md5sum"
# 두 hash 동일해야 함
```

---

## 3. Cloudflare Tunnel (Oracle Cloud HTTP receiver → Cloudflare Edge)

**왜 필요?** Oracle Cloud VM 의 receiver (port 8400) 를 **외부 인터넷에 직접 노출하지 않고** Cloudflare Pages Functions 가 HTTPS 로 접근. Workers / Pages 의 TCP 직접 연결 제약 회피 (REST Proxy 패턴, spec §7-2 SoT).

**핵심 원칙:** Tunnel 의 ingress 는 **본인 Cloudflare zone 의 서브도메인** 으로만 노출 (예: `receiver.<your-domain>`). `*.pages.dev` / `*.workers.dev` 에는 라우팅 불가.

### 3.1. Oracle VM 에 `cloudflared` 설치 (ARM64)

```bash
ssh ubuntu@<oracle-vm-ip>

# ARM64 deb 다운로드 (Ampere A1 = aarch64)
curl -L -o cloudflared.deb \
  https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64.deb
sudo dpkg -i cloudflared.deb

cloudflared --version
# expected: cloudflared version 2024.x.x (built ...) — 2024 이상이면 OK
```

### 3.2. Tunnel 인증 (브라우저 없는 VM 환경)

```bash
# VM 에서 실행하면 URL 출력 — 로컬 브라우저에서 그 URL 열어서 zone 선택
cloudflared tunnel login

# URL: https://dash.cloudflare.com/argotunnel?...
# 브라우저에서 도메인 선택 → Authorize → ~/.cloudflared/cert.pem 자동 발급
ls -la ~/.cloudflared/cert.pem
# expected: 파일 존재 + 권한 0600
```

### 3.3. Tunnel 생성 + UUID 확보

```bash
cloudflared tunnel create scp-receiver
# expected 출력:
#   Tunnel credentials written to /home/ubuntu/.cloudflared/<UUID>.json.
#   Created tunnel scp-receiver with id <UUID>

# UUID 메모
cloudflared tunnel list
# expected: NAME=scp-receiver, ID=<UUID>, CONNECTIONS=0 (아직 미실행)
```

### 3.4. 라우팅 config 작성

```bash
# 도메인을 <YOUR_DOMAIN> 자리에 박음 (예: scp.example.com)
# Edge API 가 호출할 호스트네임 = receiver.<YOUR_DOMAIN>
cat > ~/.cloudflared/config.yml <<'EOF'
tunnel: <UUID>
credentials-file: /home/ubuntu/.cloudflared/<UUID>.json

ingress:
  - hostname: receiver.<YOUR_DOMAIN>
    service: http://localhost:8400
    originRequest:
      noTLSVerify: false
  - service: http_status:404
EOF

# DNS 라우팅 등록 (Cloudflare DNS 에 CNAME 자동 생성)
cloudflared tunnel route dns scp-receiver receiver.<YOUR_DOMAIN>
# expected: "Added CNAME receiver.<YOUR_DOMAIN> which will route to this tunnel"
```

### 3.5. systemd 서비스 등록 (재부팅 후 자동 시작)

```bash
sudo cloudflared service install
sudo systemctl enable cloudflared
sudo systemctl start cloudflared

sudo systemctl status cloudflared
# expected: Active: active (running)
```

### 3.6. 검증

```bash
# (1) Tunnel connection 상태
cloudflared tunnel info scp-receiver
# expected: CONNECTIONS=2-4 (Cloudflare edge 4 region 연결)

# (2) DNS 라우팅 검증 (로컬 macOS 에서)
dig receiver.<YOUR_DOMAIN> +short
# expected: <UUID>.cfargotunnel.com 으로 CNAME chain

# (3) HTTPS 응답 검증 (receiver 가 아직 안 떠 있으므로 502 / 404 정상)
curl -i https://receiver.<YOUR_DOMAIN>/
# expected: HTTP/2 502 (origin 부재) — Cloudflare edge 까지는 도달
#           Day 11 receiver 기동 후에는 200 / 404 (FastAPI default)

# (4) 미인증 호출이 거부되는지 확인 (Day 11 implementation 후)
# curl -X POST https://receiver.<YOUR_DOMAIN>/v1/events -d '{}'
# expected (Day 11 후): {"detail":"invalid token"} 401
```

**보안 노트:**
- Oracle Cloud VM 의 ingress security list 에서 8400 포트는 **Public Internet 노출 X** — Tunnel 로만 도달
- VM 의 ufw / firewalld 도 8400 차단 권장 (Tunnel 은 outbound 만 사용)
- 임시 검증용 `cloudflared tunnel --url http://localhost:8400` (`*.trycloudflare.com`) 도 가능하나 ephemeral — Day 11 본격 사용엔 부적합

---

## 4. VAPID 키 페어 (Web Push, Day 13 선행 준비)

**왜 필요?** Web Push 인증 — Day 13 Task 13.2 Service Worker subscription + Task 13.3 Workers Cron `alert-sender` 양쪽에서 사용. Day 11 시점에 미리 생성해 두면 Day 13 진입이 매끄러움.

### 4.1. `web-push` CLI 로 1회 생성 (로컬 macOS)

```bash
# 일회성 — 글로벌 설치 불필요
npx web-push generate-vapid-keys --json
```

출력 예시:
```json
{
  "publicKey":  "BFx5...8wT",   // 88자 base64url
  "privateKey": "abc...XYZ"     // 43자 base64url
}
```

### 4.2. `.env` 저장 (`.gitignore` 의 `.env` 확인 의무)

```bash
cat >> .env <<EOF

# Phase 1B Day 13 — Web Push VAPID
VAPID_PUBLIC_KEY=<위 publicKey>
VAPID_PRIVATE_KEY=<위 privateKey>
VAPID_SUBJECT=mailto:chadolskii@icloud.com
EOF
```

**핵심 원칙:**
- `VAPID_PRIVATE_KEY` 는 **단일 push origin (Cloudflare Workers) 한정** — 분실 시 모든 기존 subscription 무효화 + 사용자가 다시 subscribe 의무
- `VAPID_PUBLIC_KEY` 만 브라우저 측 (`PushSubscribe.tsx`) 에 노출 가능. Private 은 절대 frontend bundle 에 X
- `VAPID_SUBJECT` = mailto: 또는 https: 형식. Push service provider 가 abuse 신고 시 연락처

### 4.3. 검증

```bash
# (1) 형식 검증 — publicKey 88자, privateKey 43자 (base64url)
grep '^VAPID_PUBLIC_KEY=' .env | cut -d= -f2- | awk '{print length($0)}'
# expected: 88

grep '^VAPID_PRIVATE_KEY=' .env | cut -d= -f2- | awk '{print length($0)}'
# expected: 43

# (2) base64url 문자만 포함하는지 (영숫자 + `-` + `_`)
grep '^VAPID_PUBLIC_KEY=' .env | cut -d= -f2- | grep -Eq '^[A-Za-z0-9_-]+$' && echo OK
# expected: OK

# (3) git status 에 .env 노출 안 됨 확인
git status --short | grep '\.env$' && echo "WARNING: .env tracked!" || echo "OK: .env untracked"
# expected: OK
```

---

## 5. 셋업 완료 후 통합 체크리스트

Day 11 Task 11.1 시작 전 5건 모두 통과해야 함:

- [ ] **§1**: `wrangler whoami` 가 본인 이메일 + Account ID 표시
- [ ] **§2**: 로컬 `.env` + Oracle VM `.env` 양쪽에 `RECEIVER_TOKEN` 동일 값 + md5 일치
- [ ] **§3**: `dig receiver.<YOUR_DOMAIN> +short` 가 `<UUID>.cfargotunnel.com` 으로 resolve + `curl -i https://receiver.<YOUR_DOMAIN>/` 가 502 (origin 미기동 정상)
- [ ] **§4**: `.env` 에 `VAPID_PUBLIC_KEY` (88자) + `VAPID_PRIVATE_KEY` (43자) 둘 다 존재 + `git status` 에 `.env` 미노출
- [ ] **공통**: `.gitignore` 에 `.env` 박혀 있음 + 본 prep 단계에서 새로 추가한 secret 6종 (`CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`, `RECEIVER_TOKEN`, `ANON_UA_SALT`, `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`) git diff 에 안 보임

```bash
# 통합 검증 1줄 (모두 OK 출력해야 함)
{
  wrangler whoami 2>/dev/null | grep -q "@" && echo "§1 OK" || echo "§1 FAIL"
  [ "$(grep -c '^RECEIVER_TOKEN=' .env)" = "1" ] && echo "§2 OK" || echo "§2 FAIL"
  dig receiver.<YOUR_DOMAIN> +short 2>/dev/null | grep -q "cfargotunnel.com" && echo "§3 OK" || echo "§3 FAIL"
  [ "$(grep '^VAPID_PUBLIC_KEY=' .env | cut -d= -f2- | wc -c)" -gt 80 ] && echo "§4 OK" || echo "§4 FAIL"
  git status --short | grep -q '\.env$' && echo "공통 FAIL (.env tracked)" || echo "공통 OK"
}
```

---

## 6. 보안 / 거버넌스 노트

- **secret 위치 정책 (단일 출처)**:
  - 로컬 macOS `.env` — 개발 / 검증용. 절대 commit X
  - Cloudflare Pages secret — Edge API runtime (`RECEIVER_TOKEN`, `ANON_UA_SALT`)
  - Oracle VM `.env` — receiver runtime (`RECEIVER_TOKEN`, KAFKA_*)
  - Cloudflare Workers secret — alert-sender runtime (`VAPID_PRIVATE_KEY`, `VAPID_SUBJECT`)
- **VAPID 개인키 분실 정책**: 분실 → 즉시 신규 페어 생성 → D1 `push_subscriptions` 전체 truncate → 사용자 재구독 안내. 따라서 1Password 등에 백업 필수
- **`.env` git ignore 재확인**: `grep '^\.env$' .gitignore` 결과 1줄 출력 의무
- **Phase 1A 정착 규칙 reuse (`korean-conventions`)**: 본 prep 결과의 commit 메시지는 환경 변수 셋업이라 **본 repo 에 commit 할 코드 변경 없음**. 이후 Day 11 implementation 시점에 `.env.example` 갱신 commit 으로 placeholder 6종만 박힘 (실제 값 X)

---

## 7. 자주 발생하는 문제

| 증상 | 원인 / 조치 |
|---|---|
| `wrangler login` 후 `wrangler whoami` 가 `Not authenticated` | `~/.wrangler/config/default.toml` 권한 문제 — `chmod 600 ~/.wrangler/config/default.toml` 후 재시도 |
| `cloudflared tunnel login` URL 이 만료 (10분) | 다시 명령 실행 → 새 URL 발급 |
| `cloudflared tunnel route dns` 가 `record already exists` | 기존 CNAME 수동 삭제 (Cloudflare dashboard DNS) 후 재실행 |
| `dig receiver.<YOUR_DOMAIN>` 가 zone NS 미반영 (NXDOMAIN) | NS 변경 후 propagation 대기 (최대 24h). `dig NS <YOUR_DOMAIN> @8.8.8.8` 로 NS 확인 |
| `curl https://receiver.<YOUR_DOMAIN>/` 가 1033 / 1034 / 502 무한 | `cloudflared` 가 죽었거나 config.yml `tunnel:` UUID 오타 — `sudo journalctl -u cloudflared -n 50` 로 로그 확인 |
| `npx web-push generate-vapid-keys` 가 `command not found` | `npm install -g web-push` 후 `web-push generate-vapid-keys --json` 직접 |
| Oracle VM ssh 후 ARM 인지 모름 | `uname -m` 출력 → `aarch64` 이면 §3.1 의 `arm64.deb` 정답 |
| `.env` 가 실수로 commit | 즉시 `git filter-repo` 또는 BFG repo-cleaner — 모든 secret 회전 (regenerate) 필수 (이미 노출 가정) |

---

## 8. 본 prep 종료 후 다음 작업

- Day 11 Task 11.1 implementation 시작 (plan SoT §"Task 11.1")
  - Cloudflare Pages 프로젝트 생성 + `frontend/cloudflare-pages-functions/` 디렉토리 신설
  - `wrangler pages secret put RECEIVER_TOKEN / ANON_UA_SALT` 실행 (본 §2.2 의 deferred 명령)
- `.env.example` 갱신 commit — placeholder 6종 추가 (실제 값 X)
- 이후 Task 11.2 → 11.3 → end-to-end smoke (plan SoT Day 11 종료 게이트 5건)
