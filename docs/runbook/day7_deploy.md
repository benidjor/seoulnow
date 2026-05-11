# Day 7 배포 점검 항목 (Task 7.4)

> Phase 1A Day 7 종료 게이트 — 공개 URL 접속 시 지도 + 마커 + tooltip 정상 동작.
> 실 배포 명령 (`wrangler` / `cloudflared`) 은 본인 계정 토큰 영역. 본 점검 항목은 배포 후 검증용.
> 상세 절차는 `infra/cloudflare/README.md` 참조.

---

## 사전 가정

- Day 6 종료 + Day 7 PR α (FastAPI `/api/hotspots/areas` 200) + PR β (Leaflet 지도) 머지 완료
- 로컬 producer / streaming 4 PID 가동 중 (Gold 신선도 유지)
- `~/.cloudflared/<UUID>.json` credentials + `~/.cloudflared/config.yml` 채워짐

---

## 점검 항목 (7건)

- [ ] **FastAPI healthy (로컬)** — `curl http://localhost:8000/health` → `{"ok":true}` 반환
- [ ] **Tunnel 정상** — `cloudflared tunnel run scp-api` 가 떠 있고 `curl https://api.<your-domain>.com/health` 200 반환
- [ ] **Pages build env 정착** — `web/.env.production` 또는 `wrangler pages` Production env 에 `NEXT_PUBLIC_API_BASE=https://api.<your-domain>.com` 등록되어 있음
  - 검증: `pnpm build` 산출물 `web/out/_next/static/chunks/*.js` 안에 `localhost:8000` 문자열이 0건이어야 함
    ```bash
    cd web && grep -r "localhost:8000" out/ 2>/dev/null | head -3
    # 0 hit 기대. 1건 이상 시 build env 누락 → 재빌드.
    ```
- [ ] **Pages 배포 성공** — `wrangler pages deploy out --project-name seoul-citydata --branch main` 가 deploy URL 반환
- [ ] **마커 출력** — `https://seoul-citydata.pages.dev/` 접속 → 지도 + 핫스팟 마커 1개 이상 + tooltip 텍스트 정상
- [ ] **privacy 페이지 접근 가능** — `https://seoul-citydata.pages.dev/privacy/` 200 (Next.js `trailingSlash: true` 정책)
- [ ] **모바일 viewport** — 크롬 DevTools mobile emulator 에서 지도 pinch zoom + pan 정상

---

## 환경 분기 빠른 참조

| 환경 | API base | 빌드 명령 |
|------|---------|----------|
| dev | `http://localhost:8000` | `pnpm dev` (fallback 자동) |
| prod | `https://api.<your-domain>.com` | `NEXT_PUBLIC_API_BASE=... pnpm build` 또는 `.env.production` |

`next.config.mjs` 의 `NEXT_PUBLIC_API_BASE` 는 **static export 빌드 시점에 inline** 되므로 prod 빌드 직전 env 지정 필수.

---

## 미달 시 대응

- **Pages 배포 4시간 이상 막힘** → spec §9-1 Day 7 대체 path 적용 (`pnpm build && cd out && python -m http.server 8080` + `ngrok http 8080`). 스크린샷 + 영상으로 증빙 대체.
- **Tunnel DNS propagation 지연** → 최대 5분 대기. 그래도 안 되면 `cloudflared tunnel route dns` 재실행 후 `dig api.<your-domain>.com CNAME` 으로 cf-tunnel CNAME 정착 확인.
- **마커 0개** → 브라우저 devtools network 탭에서 `/api/hotspots/areas` 호출 URL 확인. `localhost:8000` 으로 fetch 중이면 build env inline 누락 → 점검 항목 3 회귀.

---

## 종료 게이트 (spec §6-1)

본 점검 항목 7건 전부 충족 시 Day 7 종료. fallback 발동 시 ngrok URL + 영상으로 대체하고 Day 8 진입 시 명시.
