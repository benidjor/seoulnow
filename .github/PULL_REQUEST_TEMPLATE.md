<!--
PR 제목은 Conventional Commits 형식으로 작성하세요.
예: feat(infra): Day 1 — kafka kraft + lakekeeper + minio + healthcheck

Squash merge 정책상 PR 제목이 main 의 commit 메시지가 됩니다.
-->

## 요약

<!-- 이 PR 이 무엇을 / 왜 했는지 한눈에. bullet 으로. -->

-

## 배경

<!--
이 PR 이 필요했던 맥락.
- 어떤 단계를 진행하는지 (plan 의 어느 Day, 어느 Task)
- 어떤 문제를 풀려고 했는지
- 직전 PR 또는 main 상태에서 무엇이 부족했는지
-->

-

## 기대효과

<!--
이 PR 이 머지된 후 어떤 가치가 생기는지.
- 다음 Day 가 무엇을 전제로 시작할 수 있는지
- 사용자 측면의 가시적 변화
- 운영 / 비용 / SLO 측면의 영향
-->

-

## Day X 완료 기준

<!--
plan §6-1 (Phase 1A) 또는 §7-1 (Phase 1B) 에서 해당 Day 의 종료 게이트.
체크박스로 검증.
-->

- [ ] (예) ./scripts/healthcheck.sh 4/4 healthy
- [ ] (예) Kafka 토픽 N개 생성 완료
- [ ] (예) `uv run pytest tests/unit -q` 모두 PASS
- [ ] (예) DuckDB 검증 row > 0
- [ ] (예) GitHub Actions green

## 변경 사항

<!-- 핵심 변경을 bullet 으로. 파일 단위 narrative. -->

-

## 검증

<!-- 어떻게 검증했는지. 명령어 + 결과 발췌. 코드 주석은 한국어. -->

```
$ ./scripts/healthcheck.sh
== docker compose ps ==
...
== summary ==
failed sections: 0
```

## 원안에서 달라진 점

<!--
plan 본문과 실제 구현이 달라진 부분 + 이유.
없으면 "원안과 동일." 한 줄.
표 셀이 길어지면 셀 안에서 bullet (`-`) 으로 가독성 확보.

| Plan 원안 | 실제 적용 | 이유 |
|---|---|---|
-->

| Plan 원안 | 실제 적용 | 이유 |
|---|---|---|

## 트러블슈팅 (있을 때)

<!-- 큰 이슈는 docs/portfolio/troubleshooting/ 에 별도 문서로. 여기엔 link. -->

-

## 리스크 / 롤백

<!-- 데이터 손실 가능성 / 마이그레이션 / SLO 영향 / 후속 작업 영향 / 롤백 절차 -->

## 체크리스트

- [ ] secrets commit 없음 (`.env`, credentials, VAPID keys)
- [ ] tests / healthcheck pass
- [ ] CLAUDE.md / docs / runbook 갱신 (필요 시)
- [ ] backward compatible 또는 migration plan 첨부
- [ ] commit message Conventional Commits 형식 (한글 본문, type / scope 만 영어)
- [ ] PR 크기 합리적 (1000 lines 이하, 또는 분할 어려운 이유 명시)

## 참고

<!-- plan / spec / 이전 PR / runbook 링크 -->

- Plan: `docs/superpowers/plans/phase-1a-week-1.md` Task X.X
- Spec: `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md`
