# Phase 1B Day 11 Prep — cloudflared Tunnel + shell 3 이슈

**발생일**: 2026-05-21 (Phase 1B Day 11 prep)
**관련 runbook**: [`04-cloudflare-tunnel.md`](../../runbook/phase-1b-prep/04-cloudflare-tunnel.md), [`06-secrets-and-vapid.md`](../../runbook/phase-1b-prep/06-secrets-and-vapid.md)
**관련 commits**: 인프라 셋업만 (코드 commit 0)

## 요약

cloudflared systemd 등록 시 sudo path 이슈 + shell 의 보편적 함정 (ssh 자기 자신, heredoc EOF) 3개. shell 함정은 본 prep 와 무관하게 어디서나 발생 가능.

| # | 이슈 | 진단 시간 | 해결 |
|---|---|---|---|
| 1 | `sudo cloudflared service install` 가 config 를 못 찾음 | ~10분 | config + credentials + cert 를 `/etc/cloudflared/` 로 이전 + `credentials-file:` 경로 갱신 |
| 2 | VM 에서 자기 자신에 ssh `Permission denied` | ~2분 | 검증 명령 실행 위치 (로컬 vs VM) 분리, VM 안에선 직접 명령 사용 |
| 3 | heredoc `EOF` 종료 마커 들여쓰기 함정 | ~5분 (3회 발생) | grouped echo 또는 Write tool 또는 column-0 EOF |

---

## Issue 1 — `sudo cloudflared service install` 가 config 를 못 찾음

### 증상

cloudflared Tunnel 셋업의 최종 단계 (`§4-cloudflare-tunnel.md` §8) 에서:

```bash
sudo cloudflared service install
```

```
Cannot determine default configuration path. No file [config.yml config.yaml] in [~/.cloudflared ~/.cloudflare-warp ~/cloudflare-warp /etc/cloudflared /usr/local/etc/cloudflared]
```

`~/.cloudflared/config.yml` 은 분명히 존재 (foreground `cloudflared tunnel run seoulnow-receiver` 는 정상 동작).

### 원인

`sudo` 가 `~` 를 root 의 home 으로 해석:
- 일반 사용자 (`ubuntu`) shell 의 `~` = `/home/ubuntu`
- `sudo` 명령 실행 시 `~` = `/root` (root 의 home)

따라서 `sudo cloudflared` 가 검색하는 경로 `~/.cloudflared/config.yml` 은 실제 `/root/.cloudflared/config.yml` 을 의미하지만, config 는 `/home/ubuntu/.cloudflared/config.yml` 에 있어서 못 찾음.

cloudflared 의 search path:
- `~/.cloudflared` (`/root/.cloudflared` for sudo)
- `~/.cloudflare-warp`
- `~/cloudflare-warp`
- `/etc/cloudflared`
- `/usr/local/etc/cloudflared`

해결책 = 시스템 디렉토리 `/etc/cloudflared/` 로 파일들을 이전.

### 해결

#### Step 1. `/etc/cloudflared/` 생성 + 파일 3개 복사

```bash
sudo mkdir -p /etc/cloudflared

# config.yml + credentials json + cert.pem 3개 복사
sudo cp ~/.cloudflared/config.yml /etc/cloudflared/config.yml
sudo cp ~/.cloudflared/${TUNNEL_UUID}.json /etc/cloudflared/${TUNNEL_UUID}.json
sudo cp ~/.cloudflared/cert.pem /etc/cloudflared/cert.pem

# 권한 좁힘
sudo chmod 600 /etc/cloudflared/*.json /etc/cloudflared/cert.pem
sudo chmod 644 /etc/cloudflared/config.yml
```

#### Step 2. config.yml 의 `credentials-file:` 경로 갱신

원본 config.yml 의 `credentials-file:` 가 `/home/ubuntu/.cloudflared/<UUID>.json` 로 박혀 있음. systemd 환경 (root) 에서 권한 문제 가능성 → `/etc/cloudflared/` 로 치환:

```bash
sudo sed -i 's|/home/ubuntu/.cloudflared/|/etc/cloudflared/|g' /etc/cloudflared/config.yml

# 검증
sudo cat /etc/cloudflared/config.yml
# credentials-file: 가 /etc/cloudflared/...json 으로 바뀐 것 확인
```

#### Step 3. service install 재시도

```bash
sudo cloudflared service install
```

성공 출력:
```
INF Using Systemd
INF Linux service for cloudflared installed successfully
```

### 검증

```bash
sudo systemctl status cloudflared
# expected: Active: active (running), Loaded: enabled

sudo systemctl is-active cloudflared   # → active
sudo systemctl is-enabled cloudflared  # → enabled

# 4 connections 등록 확인
sudo journalctl -u cloudflared -n 20 --no-pager | grep "Registered tunnel connection" | wc -l
# expected: 4
```

로컬에서 endpoint 검증:

```bash
curl -s -o /dev/null -w "%{http_code}\n" https://receiver.seoulnow.live/
# expected: 502 (origin 부재 정상)
```

### 회피 가능 여부

`sudo` 의 `~` 해석은 unix 표준 동작이라 회피 불가. **lesson**: 시스템 service 가 사용자 config 를 참조하려면 시스템 디렉토리 (`/etc/<service>/`) 로 이전이 표준 패턴. cloudflared 의 docs 도 이 패턴을 권장.

### 대안

`sudo cloudflared --config ~/.cloudflared/config.yml service install` 처럼 `--config` 명시도 가능하나, systemd unit 파일 안의 `ExecStart` 가 `--config /etc/cloudflared/config.yml` 로 박히므로 결국 시스템 경로로 옮겨야 함.

---

## Issue 2 — VM 에서 자기 자신에 ssh `Permission denied (publickey)`

### 증상

`§2-3` 검증 명령을 VM 안에서 실행:

```bash
# VM 안에서 (ubuntu@seoulnow-receiver:~/seoulnow$)
ssh ubuntu@<PUBLIC_IP> "grep '^RECEIVER_TOKEN=' ~/seoulnow/.env | cut -d= -f2- | md5sum | awk '{print \$1}'"
```

```
The authenticity of host '<PUBLIC_IP> (<PUBLIC_IP>)' can't be established.
ED25519 key fingerprint is SHA256:K2GNXBoqGJy8ppSnYgcGebrqgsh1PvjJO7UfEdvNJY4.
Are you sure you want to continue connecting (yes/no/[fingerprint])? yes
Warning: Permanently added '<PUBLIC_IP>' (ED25519) to the list of known hosts.
ubuntu@<PUBLIC_IP>: Permission denied (publickey).
```

### 원인

VM 안에서 `ssh ubuntu@<PUBLIC_IP>` 명령은 VM 이 자기 자신에 ssh 접속 시도. 그러나:

- VM 안에 ssh **private key** 가 없음 (`~/.ssh/id_ed25519` 부재)
- VM 의 `~/.ssh/authorized_keys` 에는 **로컬 macOS 의 public key** 만 박혀 있음
- VM 자기 자신의 public key 는 `authorized_keys` 에 없음

따라서 VM → VM ssh 는 publickey 인증 실패. 정상 동작.

본 검증 명령은 **로컬 macOS 에서** 실행해야 했음. 사용자가 위치 혼동.

### 해결

#### 방법 A (권장): 로컬 macOS 에서 ssh wrapper 사용

```bash
# 로컬 macOS 에서 (VM 아님)
ssh ubuntu@<PUBLIC_IP> "grep '^RECEIVER_TOKEN=' ~/seoulnow/.env | cut -d= -f2- | md5sum | awk '{print \$1}'"
```

#### 방법 B: VM 안에서는 ssh 없이 직접 명령

```bash
# VM 안에서 (이미 그 환경에 있으므로 ssh 불필요)
grep '^RECEIVER_TOKEN=' ~/seoulnow/.env | cut -d= -f2- | md5sum | awk '{print $1}'
```

방법 B 의 awk 안의 `$1` 은 escape 없음 (단일 quote 안). 방법 A 는 ssh remote command 라 `\$1` 로 escape 필요 (shell expansion 회피).

### 검증

```bash
# 로컬 macOS
grep '^RECEIVER_TOKEN=' /Users/aryijq/Documents/01_DE_project/seoul-citydata-platform/.env | cut -d= -f2- | md5

# VM (방법 A 또는 B 중 택1)
# A: ssh ubuntu@<PUBLIC_IP> "..."
# B: VM 안에서 직접

# 두 출력이 동일 (예: 164bae3d553060ced5a50175c800e7f9) 하면 sync 성공
```

### 회피 가능 여부

근본적으로 사용자의 명령 실행 위치 혼동. **lesson**: runbook 의 검증 명령에 "로컬 macOS 에서 실행" / "VM 안에서 실행" 명시 (본 prep 후 [`06-secrets-and-vapid.md`](../../runbook/phase-1b-prep/06-secrets-and-vapid.md) §4-3 / §4-4 양쪽 모두 명시).

### 대안 — VM ↔ VM ssh 가 필요하다면

본 prep 엔 불필요하지만, 만약 VM 이 자기 자신 또는 다른 VM 에 ssh 가능해야 한다면:

```bash
# VM 안에서 ssh key 생성
ssh-keygen -t ed25519 -C "vm-self-ssh"

# 자기 자신 authorized_keys 에 public key 추가
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys

# 검증
ssh ubuntu@localhost "echo hello"
# expected: hello
```

---

## Issue 3 — heredoc `EOF` 종료 마커 들여쓰기 함정

### 증상 (3회 발생)

`cat > file <<EOF ... EOF` heredoc 종료 마커 매칭 실패. 발생 패턴 동일:

```bash
ubuntu@seoulnow-receiver:~$ cat > ~/.cloudflared/config.yml <<EOF
  tunnel: ${TUNNEL_UUID}
  credentials-file: /home/ubuntu/.cloudflared/${TUNNEL_UUID}.json
  ...
  - service: http_status:404
  EOF
> 
> exit
> ^C
```

프롬프트가 `>` 로 무한 대기. Ctrl+C 로 abort 후 같은 함정 반복.

본 prep 에서 3회 발생:
- `frontend/index.html` 생성 시 (Issue 처음 발견)
- `~/.cloudflared/config.yml` 생성 시 (재발생)
- `.env` 에 RECEIVER_TOKEN 박을 때 (회피 위해 echo 사용으로 우회)

### 원인

`<<EOF` (또는 `<<'EOF'`) heredoc 의 종료 마커는 **줄 맨 앞 (column 0)** 에 와야 함:

```bash
cat > file <<EOF
content
EOF      ← column 0, 매칭 성공
```

들여쓰기가 들어가면:

```bash
cat > file <<EOF
content
  EOF    ← column 2, 매칭 실패 → "content" 의 일부로 인식
```

shell 은 column 0 의 `EOF` 를 기다리며 무한 대기.

본 prep 의 함정: Claude 의 응답 마크다운 코드블록을 paste 할 때, 코드블록의 들여쓰기 (2칸) 가 함께 paste 됨. 사용자가 의도하지 않게 `  EOF` 형태로 입력되어 매칭 실패.

대안: `<<-EOF` (대시 포함) 는 leading **tab** 만 strip (공백은 strip 안 함). 본 케이스는 공백 들여쓰기라 `<<-` 도 해결 안 됨.

### 해결

#### 방법 A (권장 — 본 prep 채택): grouped echo

```bash
{
echo "tunnel: $TUNNEL_UUID"
echo "credentials-file: /home/ubuntu/.cloudflared/$TUNNEL_UUID.json"
echo ""
echo "ingress:"
echo "  - hostname: receiver.seoulnow.live"
echo "    service: http://localhost:8400"
echo "    originRequest:"
echo "      noTLSVerify: false"
echo "  - service: http_status:404"
} > ~/.cloudflared/config.yml
```

11줄 한 번에 paste → 마지막 `}` 줄에서 실행. heredoc 매칭 문제 0.

#### 방법 B: Claude 의 Write tool 직접 사용 (로컬 파일만 가능)

본 prep 의 `frontend/index.html` 생성 시 채택. Claude 가 로컬 파일시스템에 직접 write 하므로 heredoc 함정 회피.

```python
# Claude Code 의 Write tool 사용
Write(file_path="/path/to/file", content="...")
```

VM 안의 파일은 사용 불가 (Claude 가 VM 의 fs 에 직접 접근 안 됨).

#### 방법 C: column-0 EOF 보장

paste 시 줄 맨 앞에 공백 없도록 손으로 정리. 들여쓰기 박힌 코드블록은 미리 들여쓰기 제거 후 paste.

```bash
# 정리 후 paste
cat > file <<EOF
content
EOF
```

#### 방법 D: printf 한 줄

```bash
printf 'tunnel: %s\ncredentials-file: /home/ubuntu/.cloudflared/%s.json\n\ningress:\n  - hostname: receiver.seoulnow.live\n    service: http://localhost:8400\n    originRequest:\n      noTLSVerify: false\n  - service: http_status:404\n' "$TUNNEL_UUID" "$TUNNEL_UUID" > ~/.cloudflared/config.yml
```

읽기 어려움 → 가독성 측면에서 방법 A 가 좋음.

### 검증

파일 내용 + 라인 수:

```bash
cat ~/.cloudflared/config.yml
wc -l ~/.cloudflared/config.yml
# expected: 9 (또는 마지막 빈 줄 포함 9~10)
```

`tunnel:` 라인에 실제 UUID 가 박혀 있고 (변수 `$TUNNEL_UUID` 그대로면 잘못), `credentials-file:` 도 실제 경로면 정상.

### 회피 가능 여부

heredoc 의 syntax 자체는 unix 표준. paste 시 들여쓰기 추가 박힘이 함정의 본질. **lesson**: 본 prep 후 향후 runbook 에 heredoc 대안 (grouped echo / Write tool) 을 default 로 안내. heredoc 사용 시 "EOF 는 줄 맨 앞에" 명시.

### Claude Code 측 lesson

본 prep 에서 Claude 가 처음 heredoc 코드블록 제시 → 사용자 paste → 들여쓰기 박힘 → 함정. 이후 Claude 가 grouped echo + Write tool 로 전환. **앞으로 비슷한 상황에서는 처음부터 grouped echo / Write tool 권장.**

---

## 통합 lesson learned

1. **sudo + `~` 해석은 root home 으로** — 시스템 service 의 config 는 `/etc/<service>/` 표준 (Issue 1)
2. **VM 안 명령 실행 위치 명시 의무** — runbook 에 "로컬 macOS" / "VM 안" 명확히 (Issue 2)
3. **heredoc 은 paste 함정 — grouped echo 또는 Write tool 이 더 안전** (Issue 3)

## 관련 문서

- runbook: [`04-cloudflare-tunnel.md`](../../runbook/phase-1b-prep/04-cloudflare-tunnel.md), [`06-secrets-and-vapid.md`](../../runbook/phase-1b-prep/06-secrets-and-vapid.md)
- 다른 troubleshooting: [`2026-05-21-day-11-prep-cloudflare-account-issues.md`](./2026-05-21-day-11-prep-cloudflare-account-issues.md), [`2026-05-21-day-11-prep-oracle-cloud-issues.md`](./2026-05-21-day-11-prep-oracle-cloud-issues.md)
