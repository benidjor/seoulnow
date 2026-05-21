# 04 — Cloudflare Tunnel (Oracle VM → Edge)

> **목표**: Oracle VM 의 HTTP receiver (port 8400, Day 11 Task 11.2) 를 **외부 인터넷에 직접 노출하지 않고** Cloudflare edge 에서 `receiver.seoulnow.live` 로 접근 가능하게 함
> **결과**: cloudflared 2026.5.0 + systemd `cloudflared.service` active, hostname `receiver.seoulnow.live` → `localhost:8400` 라우팅, Tokyo edge 4 connection (nrt07 + nrt12)
> **소요**: 30~45분
> **전제**: [`05-oracle-cloud-vm.md`](./05-oracle-cloud-vm.md) 완료 (Oracle VM ssh 가능) + [`01-domain-and-dns.md`](./01-domain-and-dns.md) 완료 (Cloudflare zone Active)

## 1. REST Proxy 패턴 — 왜 Tunnel 인가

본 프로젝트 Phase 1B 데이터 흐름:

```
Browser (Next.js click)
  ↓ POST /v1/events
Cloudflare Pages Functions (Edge API)
  ↓ HTTPS POST (Bearer auth)
receiver.seoulnow.live         ← 본 doc 의 Tunnel hostname
  ↓ Cloudflare Tunnel (outbound from VM)
Oracle VM (FastAPI receiver on localhost:8400)
  ↓ confluent-kafka producer
Kafka topic user.events.v1
```

**왜 Tunnel?** Cloudflare Pages Functions / Workers 가 TCP 직접 연결 불가 (HTTP only). 그래서:
- 다음 hop 으로 HTTPS receiver 가 필요 → FastAPI 가 Oracle VM 의 8400 에 listen
- 8400 을 public internet 에 직접 open 하면 보안 risk (DDoS / brute force) → Tunnel 로 outbound only
- Cloudflared 가 Oracle VM 에서 Cloudflare edge 로 **outbound HTTPS** 만 사용 (inbound 포트 open 0)

CLAUDE.md §3 의 "REST Proxy 패턴" 결정 SoT.

## 2. cloudflared 설치 (Oracle VM ARM64)

### 2-1. ARM64 .deb 다운로드

VM 안에서:

```bash
cd ~

curl -L -o cloudflared.deb \
  https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64.deb

ls -la cloudflared.deb
# expected: ~15~20MB
```

### 2-2. dpkg 설치

```bash
sudo dpkg -i cloudflared.deb

# 의존성 누락 시 (드물지만)
sudo apt install -f -y

# 버전 확인
cloudflared --version
# expected: cloudflared version 2026.5.0 (built ...)
```

## 3. Tunnel 인증 (`tunnel login`)

### 3-1. VM 안에서 login 명령 실행

```bash
cloudflared tunnel login
```

VM 에 브라우저가 없으므로 다음 흐름으로 인증:

1. VM 터미널에 URL 출력:
   ```
   Please open the following URL and log in with your Cloudflare account:
   https://dash.cloudflare.com/argotunnel?aud=&callback=...
   ```
2. **URL 전체를 로컬 macOS 브라우저 주소창에 paste** (URL 매우 길어 줄 바꿈 주의, 한 번에 전체 선택)
3. Cloudflare 로그인 → **`Pick a zone`** → `seoulnow.live` 선택 → **`Authorize`**
4. 성공 메시지: "You have successfully logged in. You may close this window."
5. VM 터미널에 자동 메시지:
   ```
   You have successfully logged in.
   ...credentials...saved to: /home/ubuntu/.cloudflared/cert.pem
   ```

⚠️ URL 은 약 10분 후 만료. 만료 시 `cloudflared tunnel login` 재실행해서 새 URL 발급.

### 3-2. cert.pem 검증

```bash
ls -la ~/.cloudflared/cert.pem
# expected: -rw------- 1 ubuntu ubuntu ~몇백바이트 cert.pem

head -1 ~/.cloudflared/cert.pem
# expected: -----BEGIN ARGO TUNNEL TOKEN----- (신형) 또는 -----BEGIN PRIVATE KEY----- (구형)
```

## 4. Tunnel 생성

```bash
cloudflared tunnel create seoulnow-receiver
```

예상 출력:

```
Tunnel credentials written to /home/ubuntu/.cloudflared/<UUID>.json.
Created tunnel seoulnow-receiver with id <UUID>
```

`<UUID>` (36자, 예: `03db6b10-eb75-49b4-a12a-65da62a0e60e`) 를 메모. 향후 config 와 DNS routing 에 사용.

```bash
# UUID 를 env var 로 저장 (편의)
export TUNNEL_UUID=$(cloudflared tunnel list | grep seoulnow-receiver | awk '{print $1}')
echo $TUNNEL_UUID
# expected: <UUID> 출력
```

## 5. config.yml 작성 (heredoc 함정 회피)

### 5-1. grouped echo 로 안전 작성

⚠️ heredoc EOF 들여쓰기 함정 회피를 위해 grouped echo 사용:

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

위 11줄을 한 번에 paste → 마지막 `}` 줄에서 한 방에 실행. heredoc EOF 매칭 문제 없음.

### 5-2. config.yml 검증

```bash
cat ~/.cloudflared/config.yml
```

예상 출력:

```yaml
tunnel: 03db6b10-eb75-49b4-a12a-65da62a0e60e
credentials-file: /home/ubuntu/.cloudflared/03db6b10-eb75-49b4-a12a-65da62a0e60e.json

ingress:
  - hostname: receiver.seoulnow.live
    service: http://localhost:8400
    originRequest:
      noTLSVerify: false
  - service: http_status:404
```

핵심 확인:
- `tunnel:` 에 실제 UUID 박힘 (변수 `$TUNNEL_UUID` 그대로면 잘못)
- `credentials-file:` 도 실제 경로
- **마지막 `- service: http_status:404` 필수** (catchall, 없으면 cloudflared 가 안 뜸)

## 6. DNS routing 등록

Cloudflare zone (`seoulnow.live`) 에 CNAME `receiver` → `<UUID>.cfargotunnel.com` 자동 추가:

```bash
cloudflared tunnel route dns seoulnow-receiver receiver.seoulnow.live
```

예상 출력:

```
INF Added CNAME receiver.seoulnow.live which will route to this tunnel tunnelID=03db6b10-...
```

기존 CNAME 충돌 시 (`record already exists`):
- Cloudflare dashboard → `seoulnow.live` → `DNS` → `Records` → `receiver` 검색 → `Delete` 후 명령 재실행

## 7. foreground 실행 + 검증 (systemd 전 임시 테스트)

```bash
cloudflared tunnel run seoulnow-receiver
```

예상 로그:

```
INF Starting tunnel tunnelID=03db6b10-...
INF Version 2026.5.0
INF Registered tunnel connection connIndex=0 ... location=nrt08
INF Registered tunnel connection connIndex=1 ... location=nrt10
INF Registered tunnel connection connIndex=2 ... location=nrt14
INF Registered tunnel connection connIndex=3 ... location=nrt01
```

**`Registered tunnel connection` 4개 (Tokyo edge `nrt*`)** 보이면 정상.

새 터미널에서 로컬 macOS:

```bash
curl -i https://receiver.seoulnow.live/
# expected: HTTP/2 502 + server: cloudflare + cf-ray:
```

**502 정상**입니다 — Cloudflare edge → Tunnel → Oracle VM 까지 정상, 마지막 hop 의 receiver (port 8400) 가 아직 안 떠 있어서 502. Day 11 Task 11.2 implementation 시점에 receiver 가동되면 502 → 200/404 (FastAPI default) 로 바뀜.

foreground 종료: VM 터미널에서 `Ctrl + C`.

## 8. systemd 서비스 등록 (재부팅 자동 시작)

### 8-1. config + credentials + cert 를 `/etc/cloudflared/` 로 이전

⚠️ `sudo cloudflared service install` 는 `~` 를 `/root` 로 해석해 `/home/ubuntu/.cloudflared/` 의 파일을 못 찾음 (`Cannot determine default configuration path` 에러). 해결 = 파일 3개를 시스템 디렉토리로 이전.

```bash
sudo mkdir -p /etc/cloudflared

sudo cp ~/.cloudflared/config.yml /etc/cloudflared/config.yml
sudo cp ~/.cloudflared/${TUNNEL_UUID}.json /etc/cloudflared/${TUNNEL_UUID}.json
sudo cp ~/.cloudflared/cert.pem /etc/cloudflared/cert.pem

# 권한 좁힘
sudo chmod 600 /etc/cloudflared/*.json /etc/cloudflared/cert.pem
sudo chmod 644 /etc/cloudflared/config.yml
```

트러블슈팅 → [`2026-05-21-day-11-prep-tunnel-and-shell-issues.md#issue-1`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-tunnel-and-shell-issues.md#issue-1--sudo-cloudflared-service-install-가-config-를-못-찾음)

### 8-2. config.yml 의 credentials-file 경로 갱신

```bash
sudo sed -i 's|/home/ubuntu/.cloudflared/|/etc/cloudflared/|g' /etc/cloudflared/config.yml

# 검증
sudo cat /etc/cloudflared/config.yml
# credentials-file: 가 /etc/cloudflared/...json 으로 바뀐 것 확인
```

### 8-3. service install

```bash
sudo cloudflared service install
```

예상 출력:

```
INF Using Systemd
INF Linux service for cloudflared installed successfully
```

이 명령이 자동으로:
- `/etc/systemd/system/cloudflared.service` 생성
- `systemctl enable cloudflared` (부팅 자동 시작)
- `systemctl start cloudflared` (즉시 시작)

## 9. 검증 명령

### 9-1. systemd 서비스 상태 (VM 안)

```bash
sudo systemctl status cloudflared
```

핵심 확인:
- `Loaded: loaded (...; enabled; preset: enabled)` ← 부팅 자동 시작
- `Active: active (running)` ← 현재 가동
- 마지막 로그에 `Registered tunnel connection` 4개

```bash
sudo systemctl is-active cloudflared   # → active
sudo systemctl is-enabled cloudflared  # → enabled
```

### 9-2. Tunnel 정보 (VM 안)

```bash
cloudflared tunnel list
# expected: NAME=seoulnow-receiver, CONNECTIONS=2xnrt07, 2xnrt12 (또는 4 connections 표시)
```

### 9-3. DNS resolution (로컬 macOS)

```bash
dig receiver.seoulnow.live @1.1.1.1 +short
# expected: 172.67.x.x, 104.21.x.x (Cloudflare proxied anycast IP)
# 또는 <UUID>.cfargotunnel.com (proxy 미적용 시)
```

### 9-4. HTTPS endpoint (로컬 macOS)

```bash
curl -s -o /dev/null -w "%{http_code}\n" https://receiver.seoulnow.live/
# expected: 502 (origin 부재 — Day 11 receiver 가동 전 정상)

# 헤더 확인
curl -sI https://receiver.seoulnow.live/ | grep -E "^(HTTP|cf-ray|server)"
# expected: HTTP/2 502 + cf-ray: ... + server: cloudflare
```

`cf-ray` 헤더 존재 = Cloudflare edge 까지 도달, `502` = Tunnel 정상 + origin 부재.

### 9-5. systemd journal 로 최근 로그

```bash
sudo journalctl -u cloudflared -n 30 --no-pager
# 또는 follow 모드
sudo journalctl -u cloudflared -f
```

## 10. 무시 가능한 경고 2가지

| 경고 | 의미 | 조치 |
|---|---|---|
| `ICMP proxy feature is disabled (Group ID 1001 not in ping_group_range)` | cloudflared 의 ping forwarding 비활성 | ❌ 본 프로젝트 ping 안 씀, 무시 |
| `failed to sufficiently increase receive buffer size` (QUIC buffer) | UDP receive buffer 가 OS 기본값보다 작음 | ❌ 본 프로젝트 트래픽 규모 무관, 무시. Phase 2 W7 튜닝 항목 |

## 11. 운영 명령 (참고)

```bash
# 재시작
sudo systemctl restart cloudflared

# 중지 (시작은 systemctl start)
sudo systemctl stop cloudflared

# 부팅 자동 시작 해제
sudo systemctl disable cloudflared

# 완전 제거 (서비스 + binary)
sudo systemctl stop cloudflared
sudo systemctl disable cloudflared
sudo cloudflared service uninstall
sudo apt remove cloudflared

# Tunnel 자체 삭제 (Cloudflare 측, 활성 connection 없을 때)
cloudflared tunnel delete seoulnow-receiver
```

## 12. 관련 troubleshooting

[`2026-05-21-day-11-prep-tunnel-and-shell-issues.md`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-tunnel-and-shell-issues.md):
- Issue 1: `sudo cloudflared service install` 가 config 를 못 찾음 (~ → /root)
- Issue 3: heredoc EOF 들여쓰기 함정 (paste 함정)

## 13. 다음 단계

→ [`06-secrets-and-vapid.md`](./06-secrets-and-vapid.md) — `.env` (로컬 + VM) sync + VAPID 키 생성
