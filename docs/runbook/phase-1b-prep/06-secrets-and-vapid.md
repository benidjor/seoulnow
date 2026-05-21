# 06 — Secrets (RECEIVER_TOKEN / ANON_UA_SALT) + VAPID 키

> **목표**: 로컬 macOS 의 `.env` 에 secret 7종 박기 + VM 의 `.env` 에 `RECEIVER_TOKEN` sync (md5 일치 검증) + VAPID 키 페어 생성 (Day 13 사전 발급)
> **결과**: 로컬 `.env` 7 entry, VM `~/seoulnow/.env` 1 entry, md5 일치 확인
> **소요**: 20분
> **전제**: [`02-cloudflare-account-and-api-token.md`](./02-cloudflare-account-and-api-token.md) 완료 ( `CLOUDFLARE_API_TOKEN`/`CLOUDFLARE_ACCOUNT_ID` 이미 박힘) + [`05-oracle-cloud-vm.md`](./05-oracle-cloud-vm.md) 완료 (VM ssh + git clone)

## 1. `.env` 구조 — 로컬 vs VM 분담

| Entry | 로컬 `.env` | VM `~/seoulnow/.env` | 용도 |
|---|---|---|---|
| `CLOUDFLARE_API_TOKEN` | ✅ | ❌ | Pages secret 등록 (`wrangler pages secret put`) / 향후 CI |
| `CLOUDFLARE_ACCOUNT_ID` | ✅ | ❌ | wrangler 명령 / API 호출 |
| **`RECEIVER_TOKEN`** | ✅ | ✅ (동일 값, md5 일치 의무) | Edge API ↔ receiver Bearer 인증 |
| `ANON_UA_SALT` | ✅ | ❌ | ua_hash SHA-256 salt (Pages Functions secret) |
| `VAPID_PUBLIC_KEY` (87자) | ✅ | ❌ | Service Worker subscription (frontend bundle 노출 OK) |
| `VAPID_PRIVATE_KEY` (43자) | ✅ | ❌ | Workers Cron alert-sender push 서명 (절대 frontend 노출 X) |
| `VAPID_SUBJECT` | ✅ | ❌ | mailto: contact (push provider abuse 신고용) |

VM 은 receiver runtime 의 Bearer 검증만 하므로 `RECEIVER_TOKEN` 만 필요. 나머지는 Cloudflare 측 (Pages secret / Workers secret) 또는 로컬 ops 용.

## 2. `.gitignore` 안전장치 (필수, 반드시 먼저 확인)

```bash
cd /Users/aryijq/Documents/01_DE_project/seoul-citydata-platform

grep -E '^\.env$' .gitignore && echo "✓ .env 차단됨" || echo "✗ .gitignore 에 .env 추가 의무"
```

`✓` 아니면:

```bash
echo ".env" >> .gitignore
git add .gitignore
git commit -m "chore: .gitignore 에 .env 추가"
```

## 3. RECEIVER_TOKEN + ANON_UA_SALT 생성 (로컬 macOS)

### 3-1. 32-byte 고엔트로피 secret 2종 생성

```bash
# RECEIVER_TOKEN — Edge API ↔ receiver Bearer
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
# 출력 예 (43자 base64url): xy7Q8wJ-vB3PqR2NmKjL...

# ANON_UA_SALT — ua_hash salt
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
# 다른 값으로 1회 더 생성 (재사용 금지)
```

두 값을 메모장 / 1Password / macOS Keychain 등에 임시 보관 (**대화창 / commit 절대 X**).

### 3-2. 로컬 `.env` 저장

```bash
# <값> 자리에 §3-1 출력 paste
echo "RECEIVER_TOKEN=<§3-1 RECEIVER_TOKEN 값>" >> .env
echo "ANON_UA_SALT=<§3-1 ANON_UA_SALT 값>" >> .env

chmod 600 .env
```

### 3-3. 검증

```bash
# 각 entry 1개씩
grep -c '^RECEIVER_TOKEN=' .env       # → 1
grep -c '^ANON_UA_SALT=' .env         # → 1

# 길이 검증 (token_urlsafe(32) = 43자 base64url)
grep '^RECEIVER_TOKEN=' .env | cut -d= -f2- | awk '{print length($0)}'  # → 43
grep '^ANON_UA_SALT=' .env | cut -d= -f2- | awk '{print length($0)}'   # → 43

# 4 entry 모두 박혀 있는지
grep -cE '^(RECEIVER_TOKEN|ANON_UA_SALT|CLOUDFLARE_API_TOKEN|CLOUDFLARE_ACCOUNT_ID)=' .env  # → 4

# git 추적 안 됨
git ls-files .env  # 빈 출력
```

## 4. RECEIVER_TOKEN 을 VM 으로 sync (ssh pipe, 값 비노출)

### 4-1. 로컬에서 ssh pipe 로 전송

```bash
# 로컬 macOS 에서 (VM 아님!)
cd /Users/aryijq/Documents/01_DE_project/seoul-citydata-platform

# RECEIVER_TOKEN 1줄만 추출 → ssh pipe 로 VM .env 에 append
grep '^RECEIVER_TOKEN=' .env | ssh ubuntu@<PUBLIC_IP> 'cat >> ~/seoulnow/.env'
```

이 명령의 보안 특성:
- 로컬 `.env` 에서 `RECEIVER_TOKEN=` 1줄만 추출
- ssh tunnel (암호화) 통해 VM 의 `~/seoulnow/.env` 끝에 append
- **토큰 값이 화면 / 대화창 / log 에 0회 노출** ✅

### 4-2. (전제) VM `~/seoulnow/.env` 가 존재해야 함

[`05-oracle-cloud-vm.md`](./05-oracle-cloud-vm.md) §7-6 의 repo clone 완료 시 자동으로 빈 `.env` 또는 `.env.example` 복사본 있음. 없으면 VM 안에서:

```bash
# VM 안
cd ~/seoulnow
touch .env
chmod 600 .env
```

### 4-3. md5 일치 검증 (값 비노출)

토큰 값 자체는 안 보고, hash 만 비교:

```bash
# 로컬 macOS
grep '^RECEIVER_TOKEN=' /Users/aryijq/Documents/01_DE_project/seoul-citydata-platform/.env | cut -d= -f2- | md5

# VM (로컬에서 ssh wrapper, VM 안에서 자기 자신 ssh 불가)
ssh ubuntu@<PUBLIC_IP> "grep '^RECEIVER_TOKEN=' ~/seoulnow/.env | cut -d= -f2- | md5sum | awk '{print \$1}'"
```

두 hash 가 정확히 동일 (예: 두 명령 모두 `164bae3d553060ced5a50175c800e7f9`) 하면 sync 성공.

⚠️ VM 안에서 자기 자신에 ssh 하면 `Permission denied (publickey)` — VM 에 자기 ssh 키 없음. 트러블슈팅 → [`2026-05-21-day-11-prep-tunnel-and-shell-issues.md#issue-2`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-tunnel-and-shell-issues.md#issue-2--vm-에서-자기-자신-ssh-permission-denied)

### 4-4. VM 측 직접 검증 (대안 — VM 안에서 직접)

VM 안 (`ubuntu@seoulnow-receiver:~/seoulnow$`) 에서:

```bash
grep -c '^RECEIVER_TOKEN=' ~/seoulnow/.env  # → 1
grep '^RECEIVER_TOKEN=' ~/seoulnow/.env | cut -d= -f2- | awk '{print length($0)}'  # → 43
grep '^RECEIVER_TOKEN=' ~/seoulnow/.env | cut -d= -f2- | md5sum | awk '{print $1}'
# 출력값을 로컬 md5 와 비교
```

## 5. VAPID 키 페어 생성 (로컬 macOS, Day 13 사전 발급)

### 5-1. web-push CLI 로 1회 생성

```bash
cd /Users/aryijq/Documents/01_DE_project/seoul-citydata-platform

npx web-push generate-vapid-keys --json
# 첫 실행 시 web-push@3.x.x install 동의 → y
```

출력 형식 (값은 실행 시마다 다름, 아래는 자리표시자):

```json
{
  "publicKey":  "BFx5xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx8wT",
  "privateKey": "abcxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxXYZ"
}
```

- `publicKey` = 87자 base64url (P-256 EC 65 bytes, no padding)
- `privateKey` = 43자 base64url (32 bytes, no padding)

⚠️ 두 값 메모장에 임시 복사 (**대화창 / commit 절대 X**, private key 특히). 실값은 `.local-notes/phase-1b-prep-secrets.md` (gitignored) 에 박아두고 본 doc 엔 절대 박지 말 것.

### 5-2. `.env` 저장

```bash
echo "" >> .env
echo "# Phase 1B Day 13 — Web Push VAPID" >> .env
echo "VAPID_PUBLIC_KEY=<§5-1 publicKey 값>" >> .env
echo "VAPID_PRIVATE_KEY=<§5-1 privateKey 값>" >> .env
echo "VAPID_SUBJECT=mailto:<YOUR_EMAIL>" >> .env
```

### 5-3. 길이 검증 (publicKey **87자** / privateKey 43자)

⚠️ `web-push` 라이브러리는 base64url 패딩 없이 인코딩하므로:
- publicKey (P-256 EC, 65 bytes) → **87자** (88 은 padding 포함 가정 시, 실제는 87)
- privateKey (32 bytes) → 43자

```bash
grep '^VAPID_PUBLIC_KEY=' .env | cut -d= -f2- | awk '{print length($0)}'   # → 87
grep '^VAPID_PRIVATE_KEY=' .env | cut -d= -f2- | awk '{print length($0)}'  # → 43

# base64url 문자만 포함하는지 (A-Z a-z 0-9 - _)
grep '^VAPID_PUBLIC_KEY=' .env | cut -d= -f2- | grep -Eq '^[A-Za-z0-9_-]+$' && echo OK
grep '^VAPID_PRIVATE_KEY=' .env | cut -d= -f2- | grep -Eq '^[A-Za-z0-9_-]+$' && echo OK
```

### 5-4. VAPID 핵심 원칙

| 원칙 | 이유 |
|---|---|
| `VAPID_PRIVATE_KEY` 는 **단일 push origin (Workers) 한정** | 분실 시 모든 기존 subscription 무효화 + 사용자 재구독 의무 |
| `VAPID_PUBLIC_KEY` 만 frontend bundle 노출 OK | Service Worker subscription 등록 시 필수 |
| `VAPID_SUBJECT` = `mailto:` 또는 `https:` | Push provider abuse 신고 시 연락처 |
| 분실 시 즉시 신규 페어 생성 + D1 `push_subscriptions` truncate + 사용자 재구독 안내 | 분실 = 보안 사고. 1Password 등 백업 필수 |

## 6. 통합 검증 — `day11_prep.md §5` 체크리스트

```bash
cd /Users/aryijq/Documents/01_DE_project/seoul-citydata-platform

# §1: wrangler login + API token
wrangler whoami 2>&1 | grep -q "<CLOUDFLARE_ACCOUNT_ID>" && echo "§1-login OK" || echo "§1-login FAIL"
[ "$(grep -c '^CLOUDFLARE_API_TOKEN=' .env)" = "1" ] && echo "§1-token OK" || echo "§1-token FAIL"

# §2: 로컬 secret 2종
[ "$(grep -c '^RECEIVER_TOKEN=' .env)" = "1" ] && echo "§2-RECEIVER OK" || echo "§2-RECEIVER FAIL"
[ "$(grep -c '^ANON_UA_SALT=' .env)" = "1" ] && echo "§2-SALT OK" || echo "§2-SALT FAIL"

# §3: Tunnel hostname 502 (origin 부재 정상)
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" https://receiver.seoulnow.live/)
[ "$HTTP_CODE" = "502" ] && echo "§3 OK ($HTTP_CODE = origin 부재 정상)" || echo "§3 unexpected $HTTP_CODE"

# §4: VAPID 2종 + 길이 (87/43)
PUB_LEN=$(grep '^VAPID_PUBLIC_KEY=' .env | cut -d= -f2- | awk '{print length($0)}')
PRIV_LEN=$(grep '^VAPID_PRIVATE_KEY=' .env | cut -d= -f2- | awk '{print length($0)}')
[ "$PUB_LEN" = "87" ] && [ "$PRIV_LEN" = "43" ] && echo "§4 OK (87/43)" || echo "§4 FAIL ($PUB_LEN/$PRIV_LEN)"

# 공통: .env 가 git 추적 안 됨
git ls-files .env | grep -q '\.env$' && echo "공통 FAIL (.env tracked)" || echo "공통 OK"

# 공통: .gitignore 에 .env 박혀 있음
grep -E '^\.env$' .gitignore > /dev/null && echo "공통 .gitignore OK" || echo "공통 .gitignore FAIL"
```

VM 측:

```bash
ssh ubuntu@<PUBLIC_IP> '
[ "$(grep -c "^RECEIVER_TOKEN=" ~/seoulnow/.env)" = "1" ] && echo "§2-3 VM OK" || echo "§2-3 VM FAIL"
sudo systemctl is-active cloudflared | grep -q "active" && echo "§3-systemd OK" || echo "§3-systemd FAIL"
'
```

전체 OK 면 prep 완전 종료.

## 7. 보안 권장 — 노출 시 회전 시점

| Secret | 노출 시 회전 의무 | 회전 절차 |
|---|---|---|
| `CLOUDFLARE_API_TOKEN` | best practice 권장 (Pages/Workers/D1/Tunnel/DNS 권한, blast radius 작음) | <https://dash.cloudflare.com/profile/api-tokens> → `Roll` |
| `RECEIVER_TOKEN` | 필수 — Edge API/receiver Bearer 직접 통제 | `python3 -c "..."` 재생성 후 로컬 + VM 양쪽 교체 |
| `ANON_UA_SALT` | 권장 — 노출 시 ua_hash 추적 가능성 | 재생성 (단 기존 anon_id 와 호환 안 됨, Day 11 이전엔 무영향) |
| `VAPID_PRIVATE_KEY` | 권장 — push notification 위조 가능 | `npx web-push generate-vapid-keys` 재생성 + 기존 구독자 truncate + 재구독 |

## 8. 관련 troubleshooting

[`2026-05-21-day-11-prep-tunnel-and-shell-issues.md`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-tunnel-and-shell-issues.md):
- Issue 2: VM 에서 자기 자신에 ssh permission denied
- Issue 3: heredoc EOF 들여쓰기 함정

## 9. 다음 단계

본 prep 완료. **Day 11 implementation 시작** — `docs/superpowers/plans/phase-1b-week-3.md` Day 11 plan SoT 정독 후 Task 11.1 (Pages Functions Edge API) 진입.

Day 11 Task 11.1 시점에 추가로:
- `wrangler pages secret put RECEIVER_TOKEN --project-name=seoulnow` (로컬 `.env` 값 → Pages secret)
- `wrangler pages secret put ANON_UA_SALT --project-name=seoulnow`
