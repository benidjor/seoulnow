<!--
PR 제목은 Conventional Commits 형식으로 작성하세요.
예: feat(producers): Day 2 — 핫스팟 + 지하철 혼잡도 Kafka producer 도입

Squash merge 정책상 PR 제목이 main 의 commit 메시지가 됩니다.

섹션 헤더는 1.1.1 점 구분 번호 매기기 (## 1. / ### 1.1. / #### 1.1.1.)
-->

## 1. 개요

### 1.1. 배경

<!--
- 어떤 단계의 작업인지 (plan 의 Day / Task)
- 직전 PR 또는 main 상태에서 무엇이 부족했는지
-->

-

### 1.2. 목적

<!--
- 본 PR 을 머지하면 어떤 누락 / 문제가 직접 해소되는지 (왜 이 작업이 필요한가)
- task 의 핵심 의도 한 줄 요약
- "결과" / "기대 효과" 와의 차이 — 목적 = 왜 (의도), 결과 = 실제 한 일,
  기대 효과 = 머지 후 변화
-->

-

### 1.3. 결과

<!--
- 본 PR 이 실제로 한 일 (PR 시점의 달성 요약, 1~3 bullet)
- 핵심 산출물 / 상태 변화 한 줄
- 자세한 변경 narrative 는 §3 변경 사항 / §4 검증 으로 미루고 본 섹션은 요약만
- "목적" 과의 차이 — 목적은 의도, 결과는 그 의도에 대해 PR 이 실제 도달한 지점
-->

-

### 1.4. 기대 효과

<!--
- 머지 후 어떤 변화가 생기는지
- 다음 Day 가 무엇을 전제로 시작 가능한지
- 운영 / 비용 / SLO 측면 영향
- "결과" 와의 차이 — 결과는 PR 시점의 달성, 기대 효과는 머지 후 파급 효과
-->

-

## 2. 의사결정 & Trade-off

<!--
- 의사결정 / 대안 검토 / 트레이드오프 / 채택 사유
- 원안에서 달라진 점 (있을 때 표 1~3행, 없으면 "원안과 동일." 한 줄)
- review 라운드의 핵심 fix (Important 이상)
- 큰 트러블슈팅은 docs/portfolio/troubleshooting/ 별도 문서, 본문엔 link + 한 줄
- 자명한 코드 narrative (diff 가 보여주는 것) 는 쓰지 말 것

가독성 — 의사결정 narrative 가 길어지면 다음 헤더 깊이로 분리:
- ### 2.1. {의사결정 단위}
- #### 2.1.1. 충돌 발생 과정
- #### 2.1.2. 발견 과정
- #### 2.1.3. 코드에 미치는 영향
- #### 2.1.4. 처리 방법
- #### 2.1.5. 다른 후보 처리
필요 시 ##### 2.1.1.1 까지 사용. 긴 문장에 — / → 가 여러 번 등장하면
줄바꿈 또는 bullet 으로 쪼개기.
-->

-

| Plan 원안 | 실제 적용 | 이유 |
|---|---|---|

## 3. 변경 사항

<!-- 핵심 변경 bullet (파일 단위 narrative, 무엇이 바뀌었는지) -->

-

## 4. 검증

<!--
명령어 + 결과 발췌. 명령어 옆 # 주석은 한국어로.
스크린샷 / 메시지 샘플 / row count / SLO 측정값 등.
-->

```bash
$ uv run pytest tests/unit/ -v       # 단위 테스트 회귀 확인
...
```

## 5. 장애 시나리오 & 롤백 전략

<!--
머지 후 잘못되면 어떤 형태로 잘못될 수 있는가 + 롤백 방법.
해당하는 카테고리만:
- 데이터 손실 / 멱등성 / schema / SLO / 비용 / 보안 / 계층 (streaming / cron / batch ops) 의존
없으면 "잠재 위험 없음. git revert 로 롤백 가능." 한 줄.
-->

-

## 6. 체크리스트

<!--
PR 작성자가 실제 통과한 항목에 ☑. 미통과는 ☐ + 사유 한 줄.
사용자가 머지 전 final 검증 (작성자 self-check 의 false positive 보정).
-->

- [ ] 한 PR = 한 논리 작업 (atomicity)
- [ ] secrets commit 없음 (`.env` / credentials / VAPID 키)
- [ ] tests + lint pass (`uv run pytest` / `uv run ruff check`)
- [ ] plan / CLAUDE.md SoT 일관 (변경된 의사결정은 plan 도 동기화)
- [ ] commit message Conventional Commits 형식 (한글 본문, type / scope 영어, Co-Authored-By trailer)
- [ ] PR 크기 합리적 (순수 코드 200 LOC 권장 / 자동 생성물 제외)

## 7. 레퍼런스

<!-- plan / spec / 이전 PR / runbook / 메모리 링크 -->

- Plan:
- Spec:
