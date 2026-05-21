# 05 — Oracle Cloud VM (Tokyo, ARM Ampere A1)

> **목표**: Oracle Cloud Always Free 의 ARM Ampere A1 인스턴스 (4 OCPU / 24GB / 50GB boot) 생성 + Ubuntu 24.04 셋업 + ssh 접근 확보. 본 VM 이 Phase 1B 의 receiver (FastAPI) + Kafka + Flink + Airflow 통합 host
> **결과**: instance `seoulnow-receiver`, Reserved Public IP `<PUBLIC_IP>`, Ubuntu 24.04.4 LTS aarch64, ufw 22/tcp
> **소요**: 1~3시간 (ARM A1 capacity 운에 따라 최대 6시간)
> **전제**: 신용카드 (해외결제 가능), ssh 키 (`~/.ssh/id_ed25519`)

## 1. 사전 결정 — Region

⚠️ **Home Region 은 가입 시 영구 고정** (변경 불가, 다른 region 쓰려면 새 계정 필요).

| Region | ARM A1 capacity | Korea latency | 권장도 |
|---|---|---|---|
| **`ap-tokyo-1` (Tokyo)** ★ | 중상 | ~30ms | 본 프로젝트 채택 |
| `ap-seoul-1` (Seoul) | 하 (거의 안 잡힘) | ~5ms | 비권장 — capacity 부족 |
| `ap-chuncheon-1` | 하 | ~5ms | 비권장 |
| `us-ashburn-1` | 상 | ~180ms | latency 감수 시 OK |

본 prep 에서 Tokyo 선택 (Tokyo capacity 도 빡빡하지만 Korea region 보단 잘 잡힘).

## 2. 계정 가입

### 2-1. 가입 진입

<https://signup.cloud.oracle.com/> 진입 → `Sign Up`.

### 2-2. 입력 필드

| 항목 | 입력 |
|---|---|
| Country/Territory | South Korea |
| Email | (본인 이메일, Cloudflare 계정과 별개 OK) |
| First name / Last name | 영문 |
| Cloud Account Name | `benidjor` (tenant slug, console URL 의 일부) |
| **Home Region** | **`Asia Pacific Tokyo (ap-tokyo-1)`** ★ |
| Address | 한국 주소 영문 |
| Mobile Number | 한국 휴대폰 (+82) — SMS 인증 |

### 2-3. 결제수단 등록

| 카드 | 통과 사례 |
|---|---|
| 신한 / 우리 / KB / 현대 / 삼성 신용카드 | 대부분 OK |
| 트래블월렛 / 트래블로그 외환 | 통과 사례 있음 |
| 카카오뱅크 / 토스뱅크 체크카드 | **거부 사례 많음** |

가검증 결제 ($0 또는 $1) 후 24h 내 자동 환불.

### 2-4. 계정 활성화 대기

이메일로 "Welcome to Oracle Cloud" 도착 시점 부터 console 사용 가능 (보통 5~30분, 최대 24h).

## 3. PAYG 업그레이드 (90일 reclaim 회피)

### 3-1. 왜 필요한가

Always Free 만 사용해도 **90일간 인스턴스 미사용 시 자동 회수** (Oracle reclaim policy). PAYG 업그레이드로 reclaim 비활성화. **PAYG 라도 Always Free 한도 안에서 과금 0원**.

### 3-2. 업그레이드 절차

1. <https://cloud.oracle.com/> console 로그인 → ☰ → `Billing & Cost Management` → `Upgrade and Manage Payment`
2. **`Upgrade your account`** (좌측 `Pay As You Go` 카드 안의 버튼)
3. 모달:
   - Account type: **`Individual`** (Corporate 는 사업자등록 필요, 개인 portfolio 엔 X)
   - Tax details: **`Tax information is not available`** 체크 (주민등록번호 입력 절대 금지)
   - Terms & conditions 체크
   - `Upgrade your account` 클릭
4. 30초~24시간 대기. 이메일 `Oracle Cloud Services: your subscription(s) has been updated` 도착 시 완료.

### 3-3. 업그레이드 검증

console 우상단 프로필 → Tenancy → **`Subscription Type: Pay As You Go`** 표시 확인.

⚠️ PAYG 업그레이드 안 끝나도 **ARM A1 인스턴스 생성은 Free Trial 자격으로 즉시 가능** (Always Free 한도 안 자원). 트러블슈팅 → [`2026-05-21-day-11-prep-oracle-cloud-issues.md#issue-2`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-oracle-cloud-issues.md#issue-2--payg-업그레이드-1-5h-pending--arm-instance-생성-진행-가능)

## 4. SSH 키 페어 준비 (로컬 macOS)

### 4-1. 기존 키 확인 / 생성

```bash
ls -la ~/.ssh/id_ed25519.pub 2>/dev/null

# 없으면 생성
ssh-keygen -t ed25519 -C "<YOUR_EMAIL>" -f ~/.ssh/id_ed25519
```

### 4-2. Public key 복사

```bash
pbcopy < ~/.ssh/id_ed25519.pub
# 또는 출력 확인:
cat ~/.ssh/id_ed25519.pub
# expected: ssh-ed25519 AAAAC3Nz... <YOUR_EMAIL>
```

## 5. ARM A1 인스턴스 생성 (Always Free)

### 5-1. 진입

console ☰ → `Compute` → `Instances` → 우상단 **`Create instance`**.

### 5-2. Basic Information

| 필드 | 값 |
|---|---|
| Name | `seoulnow-receiver` |
| Create in compartment | 기본 (root) |
| Placement | `ap-tokyo-1`, AD-1 (capacity 부족 시 AD-2/3 시도) |

### 5-3. Image

`Change image` → `Canonical Ubuntu` → **`24.04`** (LTS) → ARM 빌드 자동 선택 (Shape 가 ARM 이라).

### 5-4. Shape ★ 핵심 (Always Free 한도 최대 활용)

`Change shape` → **`Ampere`** 탭 → **`VM.Standard.A1.Flex`** 선택 → **▶ 화살표 클릭해서 행 펼침** → 슬라이더 조정:

| 항목 | 값 |
|---|---|
| Number of OCPUs | **`4`** (Always Free 한도) |
| Amount of memory (GB) | **`24`** (Always Free 한도) |
| `Always Free-eligible` 배지 | 유지 확인 |

4/24 까지가 Always Free 한도. 5/25+ 부터 과금 시작.

⚠️ `▶ 화살표 안 보이면` 행 자체를 클릭해서 펼침. 슬라이더 안 보이면 화면 새로 고침 / 다른 항목 클릭 후 복귀.

### 5-5. Security

`Shielded instance` 토글 **OFF** (기본값). ARM 이미지와 호환성 문제 있을 수 있어 보안 컴플라이언스 환경 아니면 OFF.

### 5-6. Networking ★

| 필드 | 값 |
|---|---|
| Primary network | **`Create new virtual cloud network`** (기존 VCN 없음) |
| New VCN name | `vcn-seoulnow` |
| Subnet | **`Create new public subnet`** |
| New subnet name | `subnet-seoulnow-public` |
| CIDR block | `10.0.0.0/24` (기본) |
| Public IPv4 toggle | (자동 disabled, 정상) |

⚠️ Public IPv4 토글이 **disabled 로 보이지만 정상**. 인라인 subnet 생성 모드의 알려진 UI 동작. 생성 후 수동으로 Reserved IP 부여 (§6).

트러블슈팅 → [`2026-05-21-day-11-prep-oracle-cloud-issues.md#issue-1`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-oracle-cloud-issues.md#issue-1--public-ipv4-토글이-disabled--비활성화)

### 5-7. SSH keys

**`Paste public key`** 선택 → 텍스트 박스에 `~/.ssh/id_ed25519.pub` 내용 paste.

`Generate a key pair for me` 는 비권장 (별도 키 파일 관리 의무, `ssh -i ~/Downloads/...` 매번 명시 필요).

### 5-8. Storage

`Specify a custom boot volume size and performance setting` ON → 다음:

| 항목 | 값 |
|---|---|
| Boot volume size (GB) | `50` (Always Free 200GB 한도 안) |
| Boot volume performance (VPU) | `10` (Balanced, default) |

50/10 까지 Always Free.

### 5-9. Create

화면 최하단 **`Create`** 클릭.

### 5-10. 결과 시나리오

| 결과 | 다음 |
|---|---|
| `Provisioning` → `Running` (1~3분) | ✅ §6 진행 |
| `Out of capacity for shape VM.Standard.A1.Flex` | 1분 후 재시도, AD-2/3 변경. 1시간 안 잡히면 다른 시간대 (KST 02~05시, 09~10시) |

## 6. Reserved Public IP 부여 (수동)

인스턴스 생성 시 Public IPv4 토글이 disabled 였으므로 수동 부여.

### 6-1. Internet Gateway + Route 설정 (Quick action)

Instance details 페이지의 `Networking` 탭 → 하단 `Quick actions` 섹션 → **`Connect public subnet to internet`** 카드 → **`Connect`** 클릭 → 모달의 **`Create`** 클릭.

대부분 경고로 "이미 internet gateway / route 가 박혀 있음" 이라 메시지 = no-op 이지만, NSG `ig-quick-action-NSG` 신규 생성됨 (defense-in-depth, 무해).

### 6-2. Primary VNIC → IPv4 Addresses 진입

`Attached VNICs` 표 → **`seoulnow-receiver`** (Primary VNIC) 이름 클릭 (파란 링크) → VNIC details 페이지.

좌측 사이드바 `Resources` → **`IPv4 Addresses`** → 표에 primary private IP (`10.0.0.187` 같은 값) 1줄 표시.

### 6-3. Public IP 부여

`10.0.0.187` 행의 **`⋮`** 메뉴 → **`Edit`**:

| 항목 | 값 |
|---|---|
| Public IP type | **`Reserved public IP`** (★ Ephemeral 비권장 — stop 시 IP release) |
| Reserved Public IP | `Create new reserved public IP` |
| Name | `ip-seoulnow-receiver` |

`Update` → 30초~1분 후 Instance details 의 `Public IP Address` 칸에 IP 박힘 (예: **`<PUBLIC_IP>`**).

## 7. Linux 셋업 (ssh 접속 후)

### 7-1. ssh 접속 (로컬 macOS)

```bash
ssh ubuntu@<PUBLIC_IP>
# 첫 접속: fingerprint 확인 → yes
# 성공: ubuntu@seoulnow-receiver:~$ 프롬프트
```

### 7-2. 보안 업데이트 + 기본 패키지

```bash
sudo apt update && sudo apt upgrade -y
# 보통 20~22 보안 업데이트 적용, 5~10분 소요
# kernel upgrade 시 reboot 후 재접속

sudo apt install -y vim curl wget git jq net-tools
```

### 7-3. 시간대 (KST)

```bash
sudo timedatectl set-timezone Asia/Seoul
date
# expected: Fri May 22 ... KST 2026 형식
```

### 7-4. ufw firewall

Oracle minimal image 는 ufw 가 미설치. apt 설치 필요:

```bash
sudo apt install -y ufw
sudo ufw allow 22/tcp
sudo ufw enable   # y 입력
sudo ufw status
# expected: Status: active, 22/tcp ALLOW Anywhere
```

8400 (receiver port) 은 ufw 에도 추가하지 않음 — Cloudflare Tunnel outbound 만 사용.

트러블슈팅 → [`2026-05-21-day-11-prep-oracle-cloud-issues.md#issue-3`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-oracle-cloud-issues.md#issue-3--ufw-가-기본-미설치)

### 7-5. (선택) Docker 설치 — Day 11 receiver 컨테이너용

```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker ubuntu

exit
# 로컬에서 재접속
ssh ubuntu@<PUBLIC_IP>

docker --version
docker run --rm hello-world
# expected: "Hello from Docker!"
```

### 7-6. (Day 11 implementation 시점 준비) repo clone

```bash
cd ~
git clone https://github.com/benidjor/seoulnow.git
cd seoulnow
ls -la
# expected: README.md, frontend/, docs/, pyproject.toml 등
```

## 8. 검증 명령

### 8-1. ssh 접근 (로컬 macOS)

```bash
ssh ubuntu@<PUBLIC_IP> "uname -a && uptime"
# expected: aarch64 Linux + load average
```

### 8-2. VM 사양

```bash
ssh ubuntu@<PUBLIC_IP> "lsb_release -d && free -h && df -h /"
# expected: Ubuntu 24.04, Mem: 24Gi total, / 47G total
```

### 8-3. Docker (설치 시)

```bash
ssh ubuntu@<PUBLIC_IP> "docker --version && docker ps"
# expected: Docker version 27.x + 빈 컨테이너 리스트
```

### 8-4. ufw 상태

```bash
ssh ubuntu@<PUBLIC_IP> "sudo ufw status"
# expected: Status: active, 22/tcp ALLOW
```

## 9. 비용 검증 (PAYG 라도 $0 확인)

| 자원 | Always Free 한도 | 본 instance 사용량 | 청구 |
|---|---|---|---|
| ARM A1 Compute | 4 OCPU + 24 GB | 4 OCPU + 24 GB | $0 |
| Boot volume storage | 200 GB total | 50 GB | $0 |
| Boot volume performance | VPU 10 (Balanced) default | VPU 10 | $0 |
| Reserved Public IP | 1 free | 1 | $0 |
| Outbound traffic | 10 TB/월 | 일반 사용 | $0 |
| **합계** | | | **$0/월** |

console ☰ → `Billing & Cost Management` → `Cost Analysis` 에서 매월 $0.00 확인.

⚠️ 인스턴스 Create 시 `View estimated cost` 가 `$2.93/month` 로 표시되는데 이는 **list price** (Always Free 무시). 실 청구 $0. 트러블슈팅 → [`2026-05-21-day-11-prep-oracle-cloud-issues.md#issue-4`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-oracle-cloud-issues.md#issue-4--estimated-cost--2-93-month-실-청구-0)

## 10. 관련 troubleshooting

[`2026-05-21-day-11-prep-oracle-cloud-issues.md`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-oracle-cloud-issues.md):
- Issue 1: Public IPv4 토글이 disabled (인라인 subnet)
- Issue 2: PAYG 업그레이드 1.5h pending (인스턴스 생성 진행 가능)
- Issue 3: ufw 가 기본 미설치
- Issue 4: Estimated cost $2.93/month (실 청구 $0)

## 11. 다음 단계

→ [`04-cloudflare-tunnel.md`](./04-cloudflare-tunnel.md) — VM 안에서 cloudflared 설치 + Tunnel 셋업
→ [`06-secrets-and-vapid.md`](./06-secrets-and-vapid.md) — VM 의 `.env` 에 RECEIVER_TOKEN sync
