# Contributing

커밋 · PR 규칙의 정본은 [`docs/conventions/2026-10-21-commit-pr-convention.md`](./docs/conventions/2026-10-21-commit-pr-convention.md)이다.
2026-10-21 에 bullet-in 저장소의 컨벤션을 이식하면서 이 파일의 옛 본문 (Day 단위 PR · 명령형 제목 · 고정 트레일러)을 그 문서로 옮겼다.
옛 본문은 git 이력 (`git show a89f5af:CONTRIBUTING.md`)에 남아 있다.

## 요약

- **커밋** — `<type>(<scope>): 한국어 제목` + 본문 + `Refs:` + 트레일러이며, 제목은 명사형으로 끝낸다 (§1.1).
- **트레일러** — 실제 작업한 모델을 적고, 설계 · 구현 모델이 다르면 역할 라벨로 두 줄을 쓴다 (§1.3).
- **커밋 템플릿** — `.gitmessage` (`git config commit.template .gitmessage`).
- **git 신원** — `benidjor <94089198+benidjor@users.noreply.github.com>`.
- **PR** — 7섹션 한국어 본문을 `--body-file` 로 전달하고, Claude 서명은 넣지 않는다 (§2.1 · §2.7).
- **단위 · 머지** — PR = Task 이고 squash merge 하며, 머지는 사용자가 한다 (§2.5).
- **게시 전 점검** — `python3 .claude/tools/check-pr-format.py --body <파일> --title "<제목>"` (§2.2).
- **서브에이전트 지시 · 리뷰** — [`docs/conventions/2026-10-21-review-instructions.md`](./docs/conventions/2026-10-21-review-instructions.md).

## 브랜치

- **이름** — `<type>/<짧은-설명>` (예: `fix/area-code-official`, `docs/resume-health-audit`)이며, `claude --worktree` 로 연 세션은 `worktree-<이름>` 을 그대로 쓴다.
- **기준** — fetch 한 `origin/main` 에서 판다.
- **옛 이름** — 2026-05 까지의 `phase-1a/day-N-*` · `phase-1b/day-N-*` 은 그대로 둔다.

## 문서

- **위치** — 스펙 `docs/superpowers/specs/` · 계획 `docs/superpowers/plans/` · 트러블슈팅 `docs/portfolio/troubleshooting/` · 런북 `docs/runbook/`.
- **서식** — 컨벤션 §2.2 를 따르며, 2026-10-21 이후 문서는 PostToolUse 훅 (`.claude/hooks/check-doc-format.py`)과 CI 가 검사한다.
- **계획서 · 스펙** — 2026-10-21 이후 문서에는 「가정과 확인」 · 「더 단순한 꼴과 버린 이유」 절이 필요하다 (`.claude/hooks/check-plan-sections.py`).

## 보안 · 거버넌스

- **비밀값** — `.env` · 자격 증명 · VAPID 키는 커밋하지 않는다 (`.gitignore` 가 차단).
- **강제 push** — `git push --force` · `--no-verify` 는 사용자가 명시적으로 승인할 때만 쓴다.
- **main 직접 push 금지** — 모든 변경은 PR 을 거친다.
- **배포** — Cloudflare 대시보드 작업은 사용자가 직접 한다.
