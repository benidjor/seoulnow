# Contributing

본 프로젝트의 커밋 / 브랜치 / PR 컨벤션. 1인 운영이지만 portfolio 어필 + 운영 감각 어필 위해 표준 패턴을 따릅니다.

## 커밋 메시지 — Conventional Commits

```
<type>(<scope>): <subject 50자 이내, 명령형>

<body — why 중심, 72자 wrap>

Refs: <plan/spec 경로 또는 issue>
```

### type (10종 표준)

| type | 사용 예 |
|---|---|
| `feat` | 새 기능 / 새 파이프라인 / 새 모델 |
| `fix` | 버그 수정 |
| `chore` | 빌드 / 도구 / 설정 변경 (코드 외) |
| `docs` | 문서 (README / runbook / portfolio / troubleshooting) |
| `refactor` | 동작 변화 없는 구조 변경 |
| `test` | 테스트 추가 / 수정 |
| `perf` | 성능 개선 |
| `ci` | GitHub Actions / 파이프라인 |
| `build` | 의존성 (uv / pyproject / package.json) |
| `revert` | 이전 commit 되돌림 |

### scope (본 프로젝트 영역)

`kafka` `flink` `dbt` `infra` `producers` `api` `web` `lakekeeper` `postgres` `scripts` `cdc` `iceberg` `spark` `cloudflare` `env` `slo`

너무 작은 변경은 scope 생략 가능 (`docs: typo`).

### Subject 규칙
- 50자 이내
- 명령형 (`add` / `fix` — `added` / `fixed` X)
- 끝 마침표 X
- 한국어 / 영어 자유 (영어 우선 권장 — git tooling 친화적)

### Body 규칙
- 72자 wrap
- "왜" 중심 (무엇은 diff 가 보여줌)
- bullet (`-`) 자유
- review 가 발견한 이슈를 fix 한 commit 은 review 코멘트 요지 / 재현 방법 / 해결 방식 명시

### 예시

좋음:
```
fix(kafka): use grep -Fxq for fixed-string topic match

Previous "^${name}$" treats '.' as regex metachar; e.g. grep would
falsely match 'seoul_hotspot_congestion_v1' against
'^seoul.hotspot.congestion.v1$'. -Fx anchors entire line as a literal
string, eliminating the regex injection without escaping each dot.
```

나쁨:
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
Conventional Commits 형식 그대로. squash merge 시 main commit message 가 됨.

```
feat(infra): Day 1 — kafka kraft + lakekeeper + minio + healthcheck
```

### Description
`.github/PULL_REQUEST_TEMPLATE.md` 사용. 핵심 섹션:
1. **Summary** — 무엇을 / 왜
2. **Day Gate** — plan 의 종료 게이트 체크리스트
3. **Changes** — bullet
4. **Testing** — 명령 + 출력
5. **Deviations from plan** — plan 본문과 달라진 부분 + 이유 (portfolio 어필 자료)
6. **Risk / Rollback**
7. **Checklist** — secrets / tests / docs / backward compat

### Size 가이드
- ≤ 400 lines diff: 작음 (이상적)
- 400~1000: 중간 (분할 고려)
- 1000+: 큰 PR (분할 권장, 어려우면 description 에 분할 어려운 이유)

### Merge 정책
**Squash merge**. main 에 Day 단위 1 commit 만 들어감. PR 페이지에 squash 전 commit + review 코멘트 영구 보존.

### Self-review
1인 프로젝트라도 PR 생성 후 본인이 commit 단위로 review 코멘트 / check 표 남기기. portfolio 어필.

## 트러블 슈팅 기록

| 위치 | 어떤 트러블 슈팅 | detail |
|---|---|---|
| Commit body | 그 commit 이 직접 해결한 이슈 | 5~15줄 |
| PR description "Deviations" | 그 PR 의 plan 대비 차이 | 표 또는 bullet |
| `docs/portfolio/troubleshooting/<date>-<topic>.md` | 큰 이슈 (의사결정 얽힌 것) | 1~2 페이지 |
| `docs/portfolio/phase1a_v1.md` (Day 10) | 가장 임팩트 큰 3~5건 정제판 | 짧게 |

별도 troubleshooting 문서가 가치 있는 기준:
- spec / docs 와 reality 의 gap
- 의사결정 trade-off 가 있는 fix
- 운영 중 재발 가능성 있는 이슈

단순 typo / 1줄 fix 는 별도 문서 불필요.

## 보안 / 거버넌스

- `.env` / credentials / VAPID 키 commit 금지 (`.gitignore` 가 차단)
- `git push --force` / `--no-verify` 사용 금지 (사용자 명시 승인 시만)
- main 으로 직접 push 금지 — 모든 변경은 PR 경유
- Cloudflare 배포는 사용자 직접 (subagent 자동화 X)

## 참고 문서

- 프로젝트 컨텍스트: [`CLAUDE.md`](./CLAUDE.md)
- Phase 1 spec: [`docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md`](./docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md)
- Phase 1A Week 1 plan: [`docs/superpowers/plans/phase-1a-week-1.md`](./docs/superpowers/plans/phase-1a-week-1.md)
- Phase 1A Week 2 plan: [`docs/superpowers/plans/phase-1a-week-2.md`](./docs/superpowers/plans/phase-1a-week-2.md)
