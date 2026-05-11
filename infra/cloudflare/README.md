# Cloudflare 배포 가이드 (Day 7 Task 7.4)

> Phase 1A Day 7 — Next.js 정적 사이트 (Cloudflare Pages) + FastAPI 외부 노출 (Cloudflare Tunnel).
> 본 문서는 자동화 영역 (config 템플릿 + 절차) 만 다루며, 실 배포 명령 (`wrangler` / `cloudflared`) 은 본인 계정 토큰이 필요한 수동 영역.
> spec §3 (Cloudflare Pages + Tunnel) / spec §9-1 Day 7 대체 path (ngrok) 근거.

---

## Pages (정적 사이트)

Next.js `output: 'export'` 모드라 `web/out/` 정적 산출물만 업로드한다.

```bash
cd web
pnpm build       # web/out/ 생성

# 1회 셋업
npm i -g wrangler
wrangler login

# 배포 (project 이름은 1회 생성 후 고정)
wrangler pages deploy out --project-name seoul-citydata --branch main
```

### Build-time env 의무 (중요)

`web/next.config.mjs` 에서 `NEXT_PUBLIC_API_BASE` 는 `process.env.NEXT_PUBLIC_API_BASE || 'http://localhost:8000'` fallback 으로 정의되어 있다. **`output: 'export'` static export 빌드 시점에 값이 inline 되므로**, Cloudflare Pages 빌드 직전 env 를 반드시 지정해야 한다 (런타임 주입 불가).

| 환경 | `NEXT_PUBLIC_API_BASE` |
|------|------------------------|
| dev (로컬 `pnpm dev`) | `http://localhost:8000` (fallback 그대로) |
| prod (Cloudflare Pages) | `https://api.<your-domain>.com` (Cloudflare Tunnel hostname) |

prod 빌드 방법 2가지:

1. **wrangler pages env 사용** (권장)
   ```bash
   wrangler pages project create seoul-citydata   # 1회만
   # Pages dashboard 또는 wrangler 로 env 등록:
   #   Production env → NEXT_PUBLIC_API_BASE=https://api.<your-domain>.com
   # 이후 pnpm build 도 동일 env 로 실행
   NEXT_PUBLIC_API_BASE=https://api.<your-domain>.com pnpm build
   wrangler pages deploy out --project-name seoul-citydata --branch main
   ```
2. **`.env.production` 사용** (`web/.env.production` 생성, `.gitignore` 의 `.env.*.local` 패턴과 별개로 prod env 는 commit 금지)
   ```
   NEXT_PUBLIC_API_BASE=https://api.<your-domain>.com
   ```
   `pnpm build` 시 Next.js 가 자동 로드.

빌드는 로컬에서 수행 후 `out/` 만 업로드 — 빌드 inline 누락 시 배포된 사이트는 `http://localhost:8000` 으로 fetch 시도하여 mixed content / CORS 차단된다.

---

## Tunnel (FastAPI 외부 노출)

1. `brew install cloudflared`
2. `cloudflared tunnel login` → 브라우저 인증
3. `cloudflared tunnel create scp-api` → 토큰 + UUID 발급, credentials json 이 `~/.cloudflared/<UUID>.json` 으로 저장됨
4. `cp infra/cloudflare/tunnel-config.example.yml ~/.cloudflared/config.yml` 후 UUID / 도메인 값 채움
5. `cloudflared tunnel route dns scp-api api.<your-domain>.com`
6. `cloudflared tunnel run scp-api` (또는 launchd / systemd 등록)

이후 브라우저 (`https://seoul-citydata.pages.dev`) 가 `https://api.<your-domain>.com/api/hotspots/areas` 를 fetch 한다.

### CORS 정책 (TODO)

현재 `src/api/main.py` 의 CORS 설정은 `allow_origins=["*"]` wildcard 로 Phase 1A 데모 단계 적정 수준. Cloudflare Pages 도메인 확정 후 좁히기 (`allow_origins=["https://seoul-citydata.pages.dev"]`) 는 **Phase 1B 진입 전 별도 task** 로 분리. 본 Day 7 Task 7.4 범위 외.

---

## 대체 path (spec §9-1 Day 7)

Cloudflare Pages 배포에서 4시간 이상 막히면:

1. `cd web && pnpm build && cd out && python -m http.server 8080`
2. `ngrok http 8080` → 임시 도메인 1개 발급
3. docs 결과물 = 스크린샷 + 코드 + 동영상 (URL 영속성 없으므로 정적 증빙 위주)

ngrok 무료 tier 는 세션 재시작 시 도메인 바뀌므로 demo 영상 촬영 직후 즉시 capture.

---

## credentials 보안

- `~/.cloudflared/<UUID>.json` — Tunnel 인증 jwt. **절대 commit 금지**.
- Pages API token — `wrangler login` 이 OS keychain 에 저장. repo 내 `.env*` 에 박지 말 것.
- `.gitignore` 에 `.cloudflared/` + `infra/cloudflare/credentials/` + `infra/cloudflare/tunnel-config.yml` (실 값 본문) 차단 정착.
