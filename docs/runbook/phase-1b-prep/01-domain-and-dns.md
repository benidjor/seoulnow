# 01 — 도메인 + DNS 셋업

> **목표**: 본 프로젝트가 사용할 도메인 1개를 구매하고 Cloudflare zone 으로 NS delegation 완료.
> **결과**: `seoulnow.live` zone 활성화 (Cloudflare NS `kenia.ns.cloudflare.com` + `sean.ns.cloudflare.com`)
> **소요**: 30분~24시간 (NS 전파 대기, 보통 30분 내)
> **전제 (Cloudflare 계정)**: 없음 — 본 doc 진행 중 Cloudflare 계정 무료 가입 동시 수행

## 1. 도메인 결정 frame

### 1-1. 왜 도메인이 필수인가

Cloudflare Tunnel 의 ingress 는 **본인 Cloudflare zone 의 hostname** 으로만 라우팅 가능. `*.pages.dev` / `*.workers.dev` / `*.trycloudflare.com` 같은 Cloudflare 공유 zone 은 Tunnel 라우팅 불가. 따라서 본 프로젝트 (Phase 1B Day 11 의 Edge API → Tunnel → Oracle VM receiver) 진행을 위해 **본인 소유 도메인 + Cloudflare zone 등록 필수**.

### 1-2. 채택: `seoulnow.live` (Namecheap, $3.18 첫해 promo)

| 후보 검토 결과 | 평가 |
|---|---|
| Cloudflare Registrar | $12.30/년 (markup 0 atomic 가격, 갱신 안정). 단 promo 없음. NS 자동 박힘 (delegation 단계 skip) |
| Namecheap `.live` 첫해 promo $3.18 | **채택** — 첫해 비용 1/4, NS 수동 변경 1회 학습 가치 |
| Porkbun `.live` ~$1.50 첫해 | 동일 promo 패턴, Korean 결제 호환성 약함 |
| `.com` $10 | promo 없음 |
| `.dev` $12.30 | Google 운영 + HTTPS 강제, 단 promo 없음 |

`.live` 의 함정 = 갱신가 ~$22/년 (registry 도매가 자체가 비쌈). 1년 tactical (Phase 1B + 채용 시즌 ~6개월) 후 결정.

### 1-3. 브랜드 적합도

`seoulnow.live` 의 브랜드 fit:
- "Seoul, Now, Live" 로 자연 독해
- 본 프로젝트 핵심 서사 (실시간 streaming + "지금 한가한 카페" 추천) 와 직접 부합
- URL 만으로 면접관에게 streaming 프로젝트 인식 가능

## 2. 셋업 절차

### 2-1. Namecheap 계정 가입 + 도메인 구매

1. <https://www.namecheap.com/> 가입 (한국 결제수단 — 신용카드 / Paypal)
2. 검색창에 `seoulnow.live` 입력 → 가격 확인 (첫해 promo $1~$3 수준)
3. **WHOIS Privacy 무료** 옵션 체크 (Namecheap 기본 제공)
4. **Auto-Renew OFF** 권장 (`.live` 갱신가 ~$22, 1년 tactical 결정 시 만료 14일 전 email 알림 후 재결정)
5. 결제

### 2-2. Cloudflare 계정 가입 (이미 있으면 skip)

1. <https://dash.cloudflare.com/sign-up> 에서 가입 (2FA 권장)
2. 가입 직후 `Account home` 진입

### 2-3. Cloudflare 에 zone 추가

1. 우상단 **`+ Add`** 드롭다운 → **`Connect a domain`** 클릭
   - ⚠️ `Register a domain` (Cloudflare Registrar 신규 등록) 아님 — 이미 Namecheap 에서 구매했으므로
   - ⚠️ `Transfer a domain` (registrar 이전) 아님 — NS 만 옮기는 거지 소유권 이전 X
2. 도메인 입력 → `seoulnow.live` → `Continue`
3. **`Free plan`** 선택 → `Continue`
4. Cloudflare 가 기존 DNS 레코드 자동 import 시도 (신규 도메인이라 빈 리스트, 그대로 `Continue`)
5. **Cloudflare 가 NS 2개 값 표시** — 메모장에 복사 (계정마다 다름):
   ```
   kenia.ns.cloudflare.com
   sean.ns.cloudflare.com
   ```

### 2-4. Namecheap 에서 NS 변경

1. <https://ap.www.namecheap.com/> 로그인
2. 좌측 사이드바 **`Domain List`** → `seoulnow.live` 의 **`MANAGE`** 클릭
3. `Domain` 탭에서 스크롤 → **`NAMESERVERS`** 섹션
4. 드롭다운 (`Namecheap BasicDNS` 기본값) → **`Custom DNS`** 변경
5. NS 입력칸 2개에 Cloudflare 가 알려준 값 그대로 입력 (오타 / 끝 점 `.` 누락 주의)
6. 입력칸 오른쪽 **녹색 체크 (✓)** 클릭 → 저장
7. 상단 토스트 "Nameservers updated successfully" 확인

### 2-5. NS 전파 대기 (30분~24시간)

전파 보통 30분 내 완료. 최악 시 24시간. 다음 단계 (`02-cloudflare-account-and-api-token.md`) 진입 전 전파 확인 필수.

## 3. 검증 명령

### 3-1. NS 전파 확인 (로컬 macOS 터미널)

```bash
# (1) 기본 resolver
dig NS seoulnow.live +short
# expected: kenia.ns.cloudflare.com 와 sean.ns.cloudflare.com 두 줄
# 전파 전: dns1.registrar-servers.com / dns2.registrar-servers.com (Namecheap BasicDNS)
```

전파 중인 경우 다른 resolver 직격 확인:

```bash
# (2) Google DNS 직격
dig NS seoulnow.live @8.8.8.8 +short

# (3) Cloudflare DNS 직격
dig NS seoulnow.live @1.1.1.1 +short

# (4) Namecheap 권위 서버 직격 (NS 변경 자체 반영 확인)
dig NS seoulnow.live @dns1.registrar-servers.com +short
```

위 셋 중 어느 하나라도 `*.ns.cloudflare.com` 이 나오면 Namecheap 측 변경 완료, 전파만 대기.

### 3-2. 글로벌 전파 시각화

<https://www.whatsmydns.net/#NS/seoulnow.live> 접속 → 전세계 resolver 별 NS 값 지도. Cloudflare NS 가 점차 퍼지는 게 보임.

### 3-3. Cloudflare zone Active 확인 (대시보드)

Cloudflare dashboard → `seoulnow.live` → `Overview` 페이지의 메인 패널 중앙에:

```
✓ Your domain is now protected by Cloudflare
Your web traffic is proxying through Cloudflare, meaning:
- ...
```

녹색 체크 마크 + 위 문구가 표시되면 **zone Active 확정**.

추가로 우측 위젯 `DNS Setup: Full` + 상단 도메인 셀렉터 옆 `Free` 배지 (회색 별) 도 정상 시그널.

### 3-4. 통합 검증 1줄

```bash
dig NS seoulnow.live +short | grep -q "cloudflare.com" && echo "✓ Cloudflare NS 활성" || echo "✗ NS 전파 미완"
```

`✓ Cloudflare NS 활성` 출력 → 다음 doc (`02-cloudflare-account-and-api-token.md`) 진입 가능.

## 4. 관련 troubleshooting

본 단계에서 만난 이슈는 없음 (NS 변경 + 전파는 매끄럽게 진행).

단, 일반적으로 발생 가능한 이슈는 [`2026-05-21-day-11-prep-cloudflare-account-issues.md`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-cloudflare-account-issues.md) 참조.

## 5. 다음 단계

→ [`02-cloudflare-account-and-api-token.md`](./02-cloudflare-account-and-api-token.md) — Workers/Pages/D1/Zero Trust 활성화 + Wrangler CLI + API 토큰
