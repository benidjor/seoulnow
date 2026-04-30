<!--
PR 제목은 Conventional Commits 형식으로 작성하세요.
예: feat(infra): Day 1 — kafka kraft + lakekeeper + minio + healthcheck

Squash merge 정책상 PR 제목이 main 의 commit message 가 됩니다.
-->

## Summary

<!-- 1~2 문단으로 무엇을 / 왜 했는지. -->

## Day Gate

<!-- 해당 PR 이 닫는 Day 의 종료 게이트 체크리스트 (plan 본문 기준) -->

- [ ] (예) ./scripts/healthcheck.sh 4/4 healthy
- [ ] (예) Kafka topics N created
- [ ] (예) `uv run pytest tests/unit -q` 모두 PASS
- [ ] (예) DuckDB 검증 row > 0
- [ ] (예) GitHub Actions green

## Changes

<!-- 핵심 변경을 bullet 으로. 파일 단위 narrative. -->

-

## Testing

<!-- 어떻게 검증했는지. 명령어 + 결과 발췌. -->

```
$ ./scripts/healthcheck.sh
== docker compose ps ==
...
== summary ==
failed sections: 0
```

## Deviations from plan

<!--
plan 본문과 달라진 부분 + 이유. 작은 deviation 도 빠짐없이.
none 이면 "No deviations." 한 줄.

| Plan 원안 | 실제 적용 | 이유 |
|---|---|---|
-->

| Plan 원안 | 실제 적용 | 이유 |
|---|---|---|

## Troubleshooting (있을 때)

<!-- 큰 이슈는 docs/portfolio/troubleshooting/ 에 별도 문서로. 여기엔 link. -->

-

## Risk / Rollback

<!-- 데이터 손실 가능성 / 마이그레이션 / SLO 영향 / 롤백 절차 -->

## Checklist

- [ ] secrets commit 없음 (`.env`, credentials, VAPID keys)
- [ ] tests / healthcheck pass
- [ ] CLAUDE.md / docs / runbook 갱신 (필요 시)
- [ ] backward compatible 또는 migration plan 첨부
- [ ] commit message Conventional Commits 형식
- [ ] PR size 합리적 (≤ 1000 lines, 또는 분할 어려운 이유 명시)

## Refs

<!-- plan / spec / 이전 PR / runbook 링크 -->

- Plan: `docs/superpowers/plans/phase-1a-week-1.md` Task X.X
- Spec: `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md`
