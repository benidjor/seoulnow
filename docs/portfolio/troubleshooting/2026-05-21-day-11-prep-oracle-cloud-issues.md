# Phase 1B Day 11 Prep — Oracle Cloud VM 4 이슈

**발생일**: 2026-05-21 (Phase 1B Day 11 prep)
**관련 runbook**: [`05-oracle-cloud-vm.md`](../../runbook/phase-1b-prep/05-oracle-cloud-vm.md)
**관련 spec**: `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` §3 (Oracle Cloud Always Free)
**관련 commits**: 인프라 셋업만 (코드 commit 0)

## 요약

Oracle Cloud Always Free VM 셋업 중 4개 이슈. 모두 console UI / Free Tier 정책 관련, 인프라 자체 결함 X.

| # | 이슈 | 진단 시간 | 해결 |
|---|---|---|---|
| 1 | Public IPv4 토글이 disabled (인라인 subnet) | ~5분 | Create 후 Reserved Public IP 수동 부여 |
| 2 | PAYG 업그레이드 1.5h pending → 인스턴스 생성 가능 여부 | ~5분 | Free Trial 자격으로 ARM A1 생성 즉시 가능 |
| 3 | `ufw` 가 기본 미설치 | ~1분 | `sudo apt install -y ufw` 먼저 |
| 4 | Estimated cost `$2.93/month` (실 청구 $0) | ~3분 | "does not reflect any tier unit pricing" 안내문 = Free tier 무시한 list price |

---

## Issue 1 — Public IPv4 토글이 disabled (인라인 subnet 생성 모드)

### 증상

VM 생성 화면 (`Create instance`) 의 Networking 섹션에서:
- `Primary network` = `Create new virtual cloud network` (`vcn-seoulnow`)
- `Subnet` = `Create new public subnet` (`subnet-seoulnow-public`)

설정 후 `Public IPv4 address assignment` 섹션의 `Automatically assign public IPv4 address` 토글이 **회색 (disabled)** + 노란 경고:

```
⚠️ Warning
You must select a public subnet to assign a public IPv4 address.
```

토글 클릭해도 반응 없음. 그러나 subnet 은 분명히 `public subnet` 으로 생성 중.

### 원인

Oracle Cloud 의 form 로직:

| 시나리오 | 토글 상태 | Public IP 동작 |
|---|---|---|
| `Select existing subnet` + 기존 public subnet | 활성화 | 사용자 선택 |
| `Select existing subnet` + 기존 private subnet | 활성화, ON 해도 효과 없음 | private subnet 이라 unreachable |
| **`Create new public subnet`** | **disabled** | 토글로 자동 부여 안 됨 |

`Create new public subnet` 모드에서 토글이 disabled 인 건 form 의 의도된 동작이지만, 그 후 **자동 public IP 부여도 안 됨**. 즉 "subnet 은 public 이지만 instance 의 VNIC 에는 public IP 가 안 박힘". Review 페이지에서 `Public IPv4 address: No` 로 확인됨.

이를 해결하려면 Create 후 instance details 에서 수동 Reserved Public IP 부여.

### 해결

#### Step 1. 그대로 Create (Public IPv4 = No 인 상태로)

위 disabled 토글 그대로 두고 `Next` → Review → `Create`. instance Running 까지 1~3분.

#### Step 2. Internet Gateway 설정 (Quick action)

Instance details → Networking 탭 → 하단 `Quick actions` 섹션 → **`Connect public subnet to internet`** → `Connect` → 모달의 **`Create`**.

이 작업은 다음을 확인/추가:
- Internet Gateway 가 VCN 에 박혀 있는지 (인라인 VCN 생성 시 자동 박힘, 확인용)
- Route table 의 `0.0.0.0/0 → Internet Gateway` route (확인용)
- Network Security Group `ig-quick-action-NSG` 신규 생성 (defense-in-depth, 무해)

#### Step 3. Primary VNIC 에 Reserved Public IP 부여

1. `Attached VNICs` 표 → **`seoulnow-receiver`** (Primary VNIC) 이름 클릭 (파란 링크)
2. VNIC details → 좌측 사이드바 `Resources` → **`IPv4 Addresses`**
3. primary private IP (`10.0.0.187`) 행의 **`⋮`** → **`Edit`**
4. 편집 모달:
   - Public IP type: **`Reserved Public IP`** (★ Ephemeral 비권장)
   - Reserved Public IP: `Create new reserved public IP`
   - Name: `ip-seoulnow-receiver`
5. `Update`

### 검증

30초~1분 후 Instance details 의 `Primary VNIC` 섹션:

```
Public IPv4 address: <PUBLIC_IP>
Private IPv4 address: 10.0.0.187
```

로컬 ssh 접속 가능:

```bash
ssh ubuntu@<PUBLIC_IP> "uptime"
# expected: load average 1줄 출력
```

### Reserved vs Ephemeral 선택

| 옵션 | 차이 | 본 프로젝트 |
|---|---|---|
| **Reserved** ★ | 영구 고정. instance stop/start 시 IP 유지. Always Free 한도 안 무료 | 권장 (Tunnel hostname / DNS 가 IP 와 결합) |
| Ephemeral | instance stop 시 IP release → 재시작 시 새 IP | stop 안 하면 OK, 단 비상 시 risk |

### 회피 가능 여부

Oracle 의 form 한계라 회피 불가. **권장 패턴**: VM 생성 마법사 대신 Networking 메뉴에서 VCN/subnet 을 먼저 생성한 후, VM 생성 시 `Select existing subnet` 으로 진행하면 토글 활성화. 단 작업 단계 증가 → trade-off.

---

## Issue 2 — PAYG 업그레이드 1.5h pending → 인스턴스 생성 가능 여부 불확실

### 증상

Oracle Cloud Free Trial 가입 후 `Upgrade and Manage Payment` 에서 `Pay As You Go` 업그레이드 클릭. 모달에서 Individual / Tax info not available / Terms 동의 → `Upgrade your account`.

이후 화면: "Your upgrade is in progress. You will receive an email confirmation when your upgrade is completed."

**1.5시간 경과 후에도 동일 메시지**. 이메일 미도착. console 상단 노란 배너 `Free Tier account - You are in a Free Trial` 그대로.

이 상태에서 ARM A1 인스턴스 생성을 시도해도 되는가? PAYG 미완료 시 shape 가용 제한 있는가?

### 원인

Oracle PAYG 업그레이드는 카드 검증 + Oracle 내부 처리로 **공식적으로 최대 24시간** 소요. 정상 케이스 5분~2시간, 카드 검증 지연 시 더 길어짐.

핵심 깨달음: **ARM A1 인스턴스 생성 자체는 Free Trial 자격으로 즉시 가능**. PAYG 업그레이드는 다음 효과만 가짐:
- 90일 미사용 시 자동 회수 정책 비활성화 (장기 보존)
- Always Free 한도 초과분 (예: 5번째 ARM 인스턴스) 과금 활성화

본 prep 의 목적 (ARM A1 4 OCPU / 24GB 1개) 은 Always Free 한도 안 자원이므로 PAYG 완료 무관.

### 해결

PAYG 업그레이드 완료를 기다리지 않고 즉시 ARM A1 인스턴스 생성 시도:

1. ☰ → `Compute` → `Instances` → `Create instance`
2. Shape = `Ampere VM.Standard.A1.Flex` 4 OCPU / 24 GB (Always Free-eligible 배지 확인)
3. `Create`

결과 시나리오:
- `Provisioning` → `Running`: ✅ 정상 (Free Trial 자격으로 충분)
- `Out of capacity for shape VM.Standard.A1.Flex`: ARM capacity 운 부족 (PAYG 와 무관)
- `Subscription does not allow this shape`: 드물게 발생 (이 경우만 PAYG 완료 대기 필요)

본 prep 에서는 시나리오 A (즉시 성공) 로 진행. PAYG 업그레이드는 백그라운드로 계속 진행되어 약 2시간 후 이메일 도착.

### 검증

```bash
ssh ubuntu@<PUBLIC_IP> "uname -a && uptime"
# expected: aarch64 Linux + load average
```

PAYG 완료 후 console ☰ → Tenancy → `Subscription Type: Pay As You Go` 확인.

### 회피 가능 여부

Oracle 내부 처리 시간이라 회피 불가. **lesson**: PAYG 와 ARM 인스턴스 생성은 독립 작업, 병렬 진행 가능.

---

## Issue 3 — `ufw` 가 기본 미설치

### 증상

VM ssh 접속 후 firewall 설정 시:

```bash
sudo ufw allow 22/tcp
# sudo: ufw: command not found
```

### 원인

Oracle Cloud 의 Ubuntu 24.04 minimal image 는 `ufw` 가 기본 미설치. 일부 Ubuntu 배포판 / install 옵션에는 포함되지만 Oracle 의 cloud-init 기반 image 는 제외.

대안: Oracle 의 Security List (네트워크 레벨 방화벽) 가 이미 22 port 만 허용 + 나머지 차단. 따라서 ufw 없이도 보안상 OK.

### 해결

defense-in-depth 측면에서 ufw 설치 권장:

```bash
sudo apt install -y ufw

sudo ufw allow 22/tcp
sudo ufw enable   # "Command may disrupt existing ssh connections. Proceed?" → y

sudo ufw status
# expected: Status: active, 22/tcp ALLOW Anywhere + 22/tcp (v6) ALLOW Anywhere (v6)
```

8400 (receiver port) 은 ufw 에도 절대 추가 X — Cloudflare Tunnel outbound 만 사용.

### 검증

```bash
sudo ufw status verbose
# expected: Default: deny (incoming), allow (outgoing)
#           22/tcp ALLOW IN Anywhere
```

ssh 연결 끊김 없이 활성화 확인 (현재 ssh 세션 유지 중이어야 함).

### 회피 가능 여부

Oracle 의 minimal image 정책이라 회피 불가. **권장**: VM 셋업 runbook 에 `ufw 미설치 → apt install 먼저` 안내 명시 (본 prep 후 [`05-oracle-cloud-vm.md`](../../runbook/phase-1b-prep/05-oracle-cloud-vm.md) §7-4 에 반영).

---

## Issue 4 — Estimated cost `$2.93/month` (실 청구 $0)

### 증상

VM 생성 마법사 의 마지막 Review 화면에서 우하단 `View estimated cost` 클릭 시 모달:

```
Estimated cost
This estimate is for 1 instance running in the tenancy. ...

Compute instance
Boot volume        $2.93/month
Estimated total    $2.93/month
```

본 instance 는 Always Free 한도 안 (4 OCPU / 24 GB ARM + 50 GB boot volume) 인데 왜 $2.93 청구?

### 원인

Estimated cost 화면 본문의 작은 글씨에 결정적 한 줄:

> "This estimate is for 1 instance running in the tenancy. Estimated cost will increase with multiple instances running and **does not reflect any tier unit pricing**. To better understand your consumption, use the Cost Analysis tool."

**"does not reflect any tier unit pricing"** = Always Free 등 free tier 가격을 **무시한 raw list price**. PAYG 사용자가 Always Free 한도 초과분에 대해 부담할 가격을 보여줄 뿐.

실 청구 분석:

| 자원 | 사용량 | Always Free 한도 | 실 청구 |
|---|---|---|---|
| ARM A1 Compute (4 OCPU / 24 GB) | 1 instance | 4 OCPU + 24 GB 통째 무료 | **$0** |
| Boot volume storage | 50 GB | 200 GB block storage 통합 무료 | **$0** |
| Boot volume performance (VPU 10) | Balanced | default VPU 무료 | **$0** |
| Reserved Public IP | 1 | 1 free | **$0** |
| Outbound traffic | 일반 사용 | 10 TB/월 무료 | **$0** |
| **합계** | | | **$0/월** |

### 해결

Estimated cost 화면은 무시. 실 청구는 console ☰ → `Billing & Cost Management` → `Cost Analysis` 또는 `Invoices` 에서 확인.

```
console 매월 청구서: $0.00 (PAYG 라도 Always Free 한도 안 자원만 사용 시)
```

### 검증

VM 생성 후 1주~1개월 후 `Cost Analysis` 페이지 방문:
- `Total cost` 칸이 `$0.00` 또는 `< $0.01` (rounding 영향)
- 모든 line item 이 "Always Free" 또는 0 USD

### 회피 가능 여부

Oracle 의 cost calculator 가 free tier 를 반영하지 않는 design choice 라 회피 불가. **lesson**: estimator 의 작은 글씨 안내문 ("does not reflect tier unit pricing") 을 항상 읽고, 실 청구는 별도 Cost Analysis 로 확인.

---

## 통합 lesson learned

1. **Oracle 의 VM 생성 마법사가 인라인 subnet 생성 시 Public IP 부여를 자동화하지 못함** — Create 후 수동 Reserved IP 부여 패턴 정착 (Issue 1)
2. **PAYG 업그레이드는 장기 보존 목적, 인스턴스 생성과 독립** — Pending 중에도 ARM A1 시도 가능 (Issue 2)
3. **Oracle minimal image 는 ufw 미포함** — apt install 먼저 (Issue 3)
4. **Cost estimator 는 free tier 를 반영하지 않는 list price** — 실 청구는 Cost Analysis 별도 확인 (Issue 4)

## 관련 문서

- runbook: [`05-oracle-cloud-vm.md`](../../runbook/phase-1b-prep/05-oracle-cloud-vm.md)
- 다른 troubleshooting: [`2026-05-21-day-11-prep-cloudflare-account-issues.md`](./2026-05-21-day-11-prep-cloudflare-account-issues.md), [`2026-05-21-day-11-prep-tunnel-and-shell-issues.md`](./2026-05-21-day-11-prep-tunnel-and-shell-issues.md)
