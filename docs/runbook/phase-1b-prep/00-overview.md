# Phase 1B Day 11 Prep — 전체 개요

> **셋업 기간**: 2026-05-20 ~ 2026-05-21 (1 세션, 약 4시간)
> **결과**: day11_prep.md §1-§4 + §5 통합 체크리스트 7/7 통과, 코드 commit 0 (인프라만), main HEAD `54328f1` 미변경
> **다음 작업**: Day 11 Task 11.1-11.3 implementation (별도 세션)

## 0. 본 runbook 의 정체성

- **`docs/runbook/day11_prep.md`** = prep **절차 / 의도 SoT** (변경 없이 재현 가능)
- **본 폴더 (`docs/runbook/phase-1b-prep/`)** = 실제 실행 결과 + 구체 값 (<PUBLIC_IP> / NS 값 등) + 검증 명령 + 트러블슈팅 cross-link 의 **실행 SoT**

새 prep 진입 시 day11_prep.md 로 step 따라가다 막히면 본 폴더의 해당 주제 doc + troubleshooting doc 참조.

## 1. Cloudflare 사용 이유 (5 products + Zero Trust 의존성)

본 프로젝트가 Cloudflare 를 통합 platform 으로 채택한 이유 — Vercel/Netlify 대신, 그리고 AWS API Gateway + Lambda 대신.

| Cloudflare product | 용도 | Phase | 비용 |
|---|---|---|---|
| **DNS / Zone** (`seoulnow.live`) | 도메인 → 모든 endpoint (Pages / Tunnel / Workers) 통합 라우팅 | P1A 부터 (apex) | $0 (Free zone) |
| **Pages** | Next.js frontend (`seoulnow.live` apex) + **Pages Functions = Edge API** (`seoulnow.live/v1/events`) | **P1B Day 11 Task 11.1** | $0 (500 빌드/월 무료) |
| **Workers (Workers Cron)** | `alert-sender` cron (D1 → push subscribers 조회 → VAPID 사용 push 발송) | **P1B Day 13 Task 13.3** | $0 (100k 요청/일 무료) |
| **D1 (sqlite)** | 익명 사용자 북마크 + Web Push 구독 저장 (사용자 메타) | **P1B Day 12** | $0 (5GB 무료) |
| **Tunnel (cloudflared)** | Cloudflare Pages Functions → Oracle VM HTTP receiver (FastAPI port 8400) | **P1B Day 11 Task 11.2** | $0 |
| **Zero Trust** (Tunnel 의존성) | Tunnel 사용을 위해 team slug 필수 (`benidjor.cloudflareaccess.com`) | (Tunnel 부수) | $0 (50 user 무료) |

**채택 근거**:

1. **단일 계정 안에서 5 product 통합** = Vercel + Netlify + AWS Lambda + RDS + ngrok 따로 운영보다 운영 단순화
2. **Free tier 가 generous** = Phase 1B 전체를 $0 으로 운영 가능 (CLAUDE.md §3 "월 $0~$2" 정책 준수)
3. **Cold start 없는 Edge runtime** = Pages Functions / Workers 가 Lambda 보다 빠름
4. **REST Proxy 패턴 정당화** = Workers 가 TCP 직접 연결 불가 → 다음 hop 으로 HTTP receiver 가 필요 → Tunnel 로 Oracle VM 안 받음 (CLAUDE.md §3 의 REST Proxy 결정)
5. **GitHub 통합 자동 배포** = Pages 가 main push 시 자동 빌드/배포

**불채택 대안**:

- Vercel/Netlify (frontend only) → backend / DB / Tunnel 까지 cover 못함
- AWS Lambda + API Gateway → cold start + IAM 복잡도 + 과금 risk (Free Tier 12개월 한정)
- ngrok / localtunnel → 무료 plan 의 hostname 불안정 (Tunnel ingress hostname 고정 X)

## 2. 셋업 순서 (의존성 그래프)

```
1. 도메인 + DNS (Namecheap + Cloudflare zone delegation)
   ↓ (zone 활성화 필수)
2. Cloudflare 계정 + Workers/Pages/D1/Zero Trust 활성화 + Wrangler + API 토큰
   ↓ (계정 활성화 필수)
3. Pages 프로젝트 (frontend/ monorepo)              ┐ 병렬 가능
                                                    ├─→ 5. Oracle VM (Ampere A1 4/24)
4. (Day 11 implementation 시점) Custom domain 박기 ┘             ↓
                                                              6-A. cloudflared Tunnel (systemd)
                                                                   ↓
                                                              6-B. RECEIVER_TOKEN 로컬 ↔ VM .env sync
                                                                   ↓
                                                              7. VAPID 키 (독립, 언제든 가능)
```

세션 종료 후 `day11_prep.md §5` 통합 체크리스트로 일괄 검증.

## 3. 각 주제 runbook 인덱스

| 파일 | 주제 | 소요 |
|---|---|---|
| [01-domain-and-dns.md](./01-domain-and-dns.md) | Namecheap 도메인 구매 + Cloudflare zone delegation | 30~60분 (NS 전파 대기 포함) |
| [02-cloudflare-account-and-api-token.md](./02-cloudflare-account-and-api-token.md) | Cloudflare 계정 + Workers/Pages/D1/Zero Trust 활성화 + Wrangler + API 토큰 | 30분 |
| [03-cloudflare-pages-monorepo.md](./03-cloudflare-pages-monorepo.md) | Pages 프로젝트 생성 + monorepo 패턴 (frontend/ Root directory) | 15분 |
| [04-cloudflare-tunnel.md](./04-cloudflare-tunnel.md) | cloudflared 설치 (ARM64 deb) + login + tunnel + config + systemd | 30~45분 |
| [05-oracle-cloud-vm.md](./05-oracle-cloud-vm.md) | Oracle Cloud 계정 + PAYG 업그레이드 + VM 생성 + Linux 셋업 | 1~3시간 (ARM capacity 운) |
| [06-secrets-and-vapid.md](./06-secrets-and-vapid.md) | `.env` (로컬 + VM) + RECEIVER_TOKEN/ANON_UA_SALT + VAPID 키 | 20분 |

## 4. 관련 troubleshooting (실행 중 만난 이슈)

| 파일 | 주제 |
|---|---|
| [`2026-05-21-day-11-prep-cloudflare-account-issues.md`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-cloudflare-account-issues.md) | Cloudflare 계정/Pages/API 토큰 4 이슈 |
| [`2026-05-21-day-11-prep-oracle-cloud-issues.md`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-oracle-cloud-issues.md) | Oracle Cloud VM 셋업 4 이슈 |
| [`2026-05-21-day-11-prep-tunnel-and-shell-issues.md`](../../portfolio/troubleshooting/2026-05-21-day-11-prep-tunnel-and-shell-issues.md) | cloudflared Tunnel + shell heredoc 3 이슈 |

## 5. 최종 산출물 요약 (2026-05-21 시점)

| 자원 | 값 |
|---|---|
| 도메인 | `seoulnow.live` (Namecheap, $3.18 첫해 promo, Auto-Renew OFF) |
| Cloudflare NS | `kenia.ns.cloudflare.com`, `sean.ns.cloudflare.com` |
| Cloudflare Account ID | `<CLOUDFLARE_ACCOUNT_ID>` |
| Cloudflare Zero Trust team | `benidjor` (`benidjor.cloudflareaccess.com`) |
| Cloudflare Workers subdomain | `benidjor.workers.dev` |
| GitHub repo | `github.com/benidjor/seoulnow` (rename from `seoul-citydata-platform`) |
| 로컬 working directory | `/Users/aryijq/Documents/01_DE_project/seoul-citydata-platform/` (rename X — Claude Code 메모리 경로 churn 회피) |
| Pages 프로젝트 | `seoulnow` (Root directory=`frontend/`, dev URL `seoulnow.pages.dev`) |
| Oracle Cloud region | `ap-tokyo-1` (Japan East Tokyo, 영구 고정) |
| Oracle VM | `seoulnow-receiver` (ARM Ampere A1.Flex 4 OCPU / 24GB, Ubuntu 24.04.4 LTS, Reserved IP **`<PUBLIC_IP>`**) |
| Oracle VCN / Subnet | `vcn-seoulnow` / `subnet-seoulnow-public` (10.0.0.0/24) |
| cloudflared Tunnel | name=`seoulnow-receiver`, UUID=`03db6b10-eb75-49b4-a12a-65da62a0e60e`, hostname=`receiver.seoulnow.live` |
| systemd service | `cloudflared.service` (enabled, active, /etc/cloudflared/config.yml) |
| 로컬 `.env` 박힌 7 entry | `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`, `RECEIVER_TOKEN`, `ANON_UA_SALT`, `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`, `VAPID_SUBJECT` |
| VM `~/seoulnow/.env` | `RECEIVER_TOKEN` (로컬과 md5 `164bae3d553060ced5a50175c800e7f9` 일치) |

## 6. 다음 세션 진입 절차

1. MEMORY.md 자동 로드 → `phase-1b-progress.md` 통해 prep 완료 상태 확인
2. `docs/superpowers/plans/phase-1b-week-3.md` Day 11 정독 (plan SoT)
3. **Task 11.1 implementation 시작** — `frontend/functions/v1/events.ts` 신설 + `wrangler pages secret put RECEIVER_TOKEN / ANON_UA_SALT --project-name=seoulnow` 실행
4. **Task 11.2** — Oracle VM 의 `~/seoulnow/` 안에 FastAPI receiver 가동 → `curl https://receiver.seoulnow.live/` 가 502 → 200/404 로 바뀌면 성공
5. **Task 11.3** — Kafka `user.events.v1` topic 생성 + end-to-end smoke
