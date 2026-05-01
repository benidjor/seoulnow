# Contributing

본 프로젝트의 커밋 / 브랜치 / PR 컨벤션. 1인 운영이지만 운영 감각을 갖추고자 표준 패턴을 따릅니다.

## 커밋 메시지 — Conventional Commits

```
<type>(<scope>): <제목 50자 이내, 명령형>

<본문 — 왜 했는지 중심, 72자 wrap>

Refs: <plan/spec 경로 또는 issue>
```

### type (10종 표준 — 영어 그대로 사용)

| type | 사용 예 |
|---|---|
| `feat` | 새 기능 / 새 파이프라인 / 새 모델 |
| `fix` | 버그 수정 |
| `chore` | 빌드 / 도구 / 설정 변경 (코드 외) |
| `docs` | 문서 (README / runbook / 포트폴리오 / 트러블슈팅) |
| `refactor` | 동작 변화 없는 구조 변경 |
| `test` | 테스트 추가 / 수정 |
| `perf` | 성능 개선 |
| `ci` | GitHub Actions / 파이프라인 |
| `build` | 의존성 (uv / pyproject / package.json) |
| `revert` | 이전 commit 되돌림 |

### scope (본 프로젝트 영역 — 영어 그대로)

`kafka` `flink` `dbt` `infra` `producers` `api` `web` `lakekeeper` `postgres` `scripts` `cdc` `iceberg` `spark` `cloudflare` `env` `slo`

너무 작은 변경은 scope 생략 가능 (`docs: 오타 수정`).

### 제목 규칙

- 50자 이내
- 명령형 (`추가` / `수정` 같은 동사 종결)
- 끝에 마침표 X
- **type / scope 만 영어**, 나머지는 한국어로 작성

### 본문 규칙

- 72자 wrap
- "왜" 중심 (무엇은 diff 가 보여줌)
- bullet (`-`) 자유롭게
- 리뷰가 발견한 이슈를 fix 한 commit 은 리뷰 코멘트 요지 / 재현 방법 / 해결 방식을 명시
- 모든 commit 의 message body 끝에 `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>` trailer 를 박는다 (Day 2 부터 적용)

### 예시

좋은 예:

```
fix(kafka): topic 매칭 시 grep -Fxq 사용

기존 "^${name}$" 패턴은 '.' 를 정규식 메타문자로 해석해
'seoul_hotspot_congestion_v1' 같은 언더스코어 변형도
'^seoul.hotspot.congestion.v1$' 와 매치됨. -Fx 로 한 줄 전체를
literal string 으로 처리해 정규식 주입 자체를 차단.
```

나쁜 예:

```
fixed bug
```

## 브랜치 네이밍

```
phase-<phase>/day-<n>-<short-desc>
```

예시:

- `phase-1a/day-1-infra`
- `phase-1a/day-2-producers`
- `phase-1a/day-3-bronze-silver`
- `phase-1a/day-4-gold-slo`
- `phase-1a/day-5-dbt-ci`
- `phase-1a/day-6-cdc`
- `phase-1a/day-7-nextjs`
- `phase-1a/day-8-chill-demo`
- `phase-1a/day-9-spark-closure`
- `phase-1a/day-10-portfolio`
- `phase-1b/day-11-edge-events`
- ...

특수 패턴 (필요 시):

- `fix/<short>` — main 핫픽스
- `docs/<short>` — 문서 단독
- `refactor/<short>` — 큰 구조 변경

## PR 컨벤션

### 단위 — Day 단위 PR

plan 의 매 Day 종료 게이트가 자연스러운 PR merge 지점. PR 1개 = Day 1개.

### 제목

Conventional Commits 형식 그대로. squash merge 시 main commit 메시지가 됨.

```
feat(infra): Day 1 — kafka kraft + lakekeeper + minio + healthcheck
```

### 본문

`.github/PULL_REQUEST_TEMPLATE.md` 사용. 핵심 섹션 (모두 한글 소제목, bullet 우선):

1. **요약** — 이번 PR 이 무엇을 / 왜 했는지 한눈에. bullet 으로.
2. **배경** — 이 PR 이 필요했던 맥락. 어떤 문제를 풀거나 어떤 단계를 진행하는지. plan 의 어느 Task 에 해당하는지.
3. **기대효과** — 이 PR 이 머지된 후 가능해지는 것. 다음 Day 가 무엇을 전제로 시작할 수 있는지. SLO / 비용 / 운영 측면 영향.
4. **Day X 완료 기준** — plan §6-1 의 해당 Day 종료 게이트 체크리스트.
5. **변경 사항** — 파일 단위 narrative. bullet.
6. **검증** — 어떻게 확인했는지. 명령 + 출력 발췌.
7. **원안에서 달라진 점** — plan 본문과 실제 구현이 달라진 부분 + 이유. 표 안의 셀이 길어지면 bullet 으로.
8. **트러블슈팅** — 큰 이슈는 별도 문서로 archive 하고 여기엔 link.
9. **리스크 / 롤백** — 데이터 손실 가능성 / 마이그레이션 / 후속 작업 영향.
10. **체크리스트** — secrets / tests / docs / backward compat / commit 컨벤션 / PR 크기.
11. **참고** — plan / spec / 이전 PR / 관련 문서 link.

원칙:
- 영어 소제목 / 영어 narrative 금지 — type / scope / 영문 식별자 / 코드 주석은 별개.
- PR description 안의 코드 블록 안 주석은 한국어로.
- 표 셀이 두 줄 넘게 길어지면 셀 안에서 bullet (`-`) 으로 쪼개기.

### 크기 가이드

- 400 lines diff 이하: 작음 (이상적)
- 400~1000: 중간 (분할 고려)
- 1000+: 큰 PR (분할 권장, 어려우면 description 에 분할 어려운 이유)

### Merge 정책

**Squash merge**. main 에 Day 단위 1 commit 만 들어감. PR 페이지에 squash 전 commit + 리뷰 코멘트 영구 보존.

### 셀프 리뷰

1인 프로젝트라도 PR 생성 후 본인이 commit 단위로 리뷰 코멘트 / 체크 표 남기기.

## 트러블슈팅 기록

| 위치 | 어떤 트러블슈팅 | 분량 |
|---|---|---|
| Commit 본문 | 그 commit 이 직접 해결한 이슈 | 5~15줄 |
| PR description "계획 대비 편차" | 그 PR 의 plan 대비 차이 | 표 또는 bullet |
| `docs/portfolio/troubleshooting/<date>-<topic>.md` | 큰 이슈 (의사결정 얽힌 것) | 1~2 페이지 |
| `docs/portfolio/phase1a_v1.md` (Day 10) | 가장 임팩트 큰 3~5건 정제판 | 짧게 |

별도 트러블슈팅 문서가 가치 있는 기준:

- spec / docs 와 reality 의 gap
- 의사결정 trade-off 가 있는 fix
- 운영 중 재발 가능성 있는 이슈

단순 오타 / 1줄 fix 는 별도 문서 불필요.

## 보안 / 거버넌스

- `.env` / credentials / VAPID 키는 commit 금지 (`.gitignore` 가 차단)
- `git push --force` / `--no-verify` 사용 금지 (사용자 명시 승인 시만)
- main 으로 직접 push 금지 — 모든 변경은 PR 경유
- Cloudflare 배포는 사용자 직접 진행 (subagent 자동화 X)

## 참고 문서

- 프로젝트 컨텍스트: [`CLAUDE.md`](./CLAUDE.md)
- Phase 1 spec: [`docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md`](./docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md)
- Phase 1A Week 1 plan: [`docs/superpowers/plans/phase-1a-week-1.md`](./docs/superpowers/plans/phase-1a-week-1.md)
- Phase 1A Week 2 plan: [`docs/superpowers/plans/phase-1a-week-2.md`](./docs/superpowers/plans/phase-1a-week-2.md)
