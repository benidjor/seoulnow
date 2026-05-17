# Day 10 PR template 컨벤션 회귀 + plan-update drift (Task 10.1 파일명 + 4 PR amend + force push)

> 작성: 2026-05-14 KST
> 시점: Day 10 PR β (#58) + 본 세션 후속 4 PR (#60-#63) 머지 완료 후 학습 자산 명문화. Phase 1B 진입 직전 baseline 정리.
> 관련 PR: #58 (Day 10 PR β phase1a_v1 종합 리포트), #60 / #61 / #62 / #63 (본 세션 후속 4 PR)
> 관련 메모리: [`korean-conventions`](../../../../../../.claude/projects/-Users-aryijq-Documents-01-DE-project-seoul-citydata-platform/memory/korean-conventions.md) (PR template 회귀 사례 SoT) + `.github/PULL_REQUEST_TEMPLATE.md` (template 단일 출처) + `CONTRIBUTING.md` (scope list SoT)
> 동시 작성 archive (Day 10 주제 분리): [`2026-05-14-day-10-slo-redesign-and-path-b.md`](2026-05-14-day-10-slo-redesign-and-path-b.md), [`2026-05-14-day-10-flink-mini-cluster-and-backfill.md`](2026-05-14-day-10-flink-mini-cluster-and-backfill.md)

## 0. 진입 흐름 요약

본 archive 는 Day 10 PR β + 본 세션 후속 4 PR 작업 도중 발견한 학습 자산 2건 명문화:

| 학습 자산 | section | PR |
|---|---|---|
| Task 10.1 파일명 불일치 (`system_diagram.md` 명명 vs 실제 `data-flow.md` reuse + `data_lineage.md` 신규) | §1 | #58 PR β |
| PR template 컨벤션 위반 정정 사례 (4 PR amend + force push) | §2 | 본 세션 #60-#63 |

본 두 학습 자산은 본 프로젝트의 **plan SoT vs 실제 구현 drift + memory 컨벤션 vs PR body 회귀** 라는 운영 패턴의 부산물. Phase 1B Day 11-18 의 모든 PR + plan-update commit 작성 시점에 회귀 회피 의무 → 인용 의무.

---

## 1. Task 10.1 파일명 불일치 (`system_diagram.md` → `data-flow.md` reuse) — plan-update drift

### 1.1. plan SoT vs 실제 작업

plan SoT (`phase-1a-week-2.md` Task 10.1, line 2546-2589) = `docs/architecture/system_diagram.md` 신규 작성 명시. 실제 Day 10 PR β 작업 = 기존 `docs/architecture/data-flow.md` 재사용 + `data_lineage.md` 신규 작성으로 결정.

### 1.2. 결정 사유

- `data-flow.md` (Day 2 시점부터 정착) 가 이미 컴포넌트 다이어그램 보유 + Day 10 시점 갱신 필요
- 신규 file 작성 (system_diagram.md) 보다 기존 file 정정이 단일 출처 원칙 일치
- `data_lineage.md` 는 Medallion 레이어 별 데이터 흐름 (Bronze → Silver → Gold mart 매핑) — 새 관점 file 로 신설 자연

### 1.3. 결과 — plan SoT drift

plan SoT 의 file name 이 실제와 불일치 → PR δ (#62) 의 plan-update commit 에서 §3 정정으로 명시 (line 본문 2546 그대로 두고 Self-Review 직전 새 section 에 정정 본문 박음).

![plan SoT (phase-1a-week-2.md) 의 Self-Review 직전 PR δ 정정 sub-section — Task 10.1 파일명 불일치 본문](./2026-05-14-day-10-pr-convention-regression/screenshots/05-plan-update-drift-sub-section.png)

### 1.4. 학습 — plan-update commit 의 의도

plan SoT 가 작성 시점 (Day 5-6 buffer) 의 의도 vs 실제 Day 10 작업 시점의 결정 차이 = plan-update commit 으로 일관성 회복. plan SoT 본문 직접 정정 X (history 보존), 새 sub-section 에 변경 본문 박음.

본 패턴 = `[[airflow-decision]]` 메모리 SoT 의 점진적 plan-update 작성 패턴과 일관. Phase 1B Week 3 plan (`phase-1b-week-3.md`) 의 Task 11.1-18.2 모두 골격만 박힘 → 각 Day 진입 직전 plan-update commit 으로 상세 step 작성 + drift 발견 시 동일 sub-section 패턴 적용.

---

## 2. PR template 컨벤션 위반 정정 사례 — 4 PR amend + force push

### 2.1. 첫 작성 시 누락 5건

본 세션 4 PR (#60-#63) 첫 작성 시 메모리 `korean-conventions` + `.github/PULL_REQUEST_TEMPLATE.md` 정공 구조 미준수:

| 위반 | 누락 / 오류 |
|---|---|
| §1.1-1.4 sub-section | §1 개요 안 1.1 배경 / 1.2 목적 / 1.3 결과 / 1.4 기대 효과 sub-section 모두 누락 |
| §5 장애 시나리오 & 롤백 | feature PR 필수 section 누락 |
| §6 체크리스트 | PR template SoT 6 항목 체크리스트 누락 |
| §7 레퍼런스 번호 매핑 | §5 후속 / §6 레퍼런스 = template 의 §5 장애 / §6 체크리스트 자리 잘못 박힘 |
| commit scope | `docs(portfolio)` / `docs(plan)` 사용, CONTRIBUTING.md scope list (`kafka / flink / dbt / infra / ...`) 외 |

![PR template SoT (.github/PULL_REQUEST_TEMPLATE.md) 의 §1.1-1.4 / §5 / §6 sub-section 강조](./2026-05-14-day-10-pr-convention-regression/screenshots/04-pr-template-sot-structure.png)

### 2.2. jargon 위반 다수

메모리 `korean-conventions` 의 외래어 풀이 명세 위반:

| jargon | 풀이 |
|---|---|
| `closure` | "해결 완료" / "보강" (case 별) |
| `deviation` | "Plan 대비 변경" / "우회안" / "보강 사항" |
| `atomicity` | "한 PR = 한 논리 작업" |
| `scope` | "범위" |
| `LOC` | "줄" |
| `placeholder` | "비워둔 본문" |
| `portfolio` | "phase1a_v1" / "리포트" |
| `정공 path` | "표준 path" |
| `forward compatibility` | "호환 진화" |
| `narrative` | "본문" |

### 2.3. grep self-check 명세 자체가 grep target

PR 본문 검증 section 의 grep 명령 자체가 매칭 keyword 포함 → grep self-check 회귀:

```
# 위반 본문 예시 (회피 의무):
grep -nE "포트폴리오|어필|면접|JD|이력서|취업|회고|narrative" — 매칭 N건
```

추상화 정공:

```
메모리 컨벤션 grep self-check 명세 적용 — 매칭 N건 (기존 본문 인용 유지)
```

### 2.4. 정정 진행 — 4 PR amend + force push

명시 승인 후 진행:

```bash
# 1. 4 PR body 새 본문 작성 (정공 구조 + jargon 풀어쓰기)
/tmp/scp-pr60-v2.md / pr61-v2 / pr62-v2 / pr63-v2

# 2. 각 file grep self-check 통과 확인
# - 절대 금지 표현 = 0건 (기존 본문 인용 외)
# - jargon = 0건
# - tilde strike-through = 0건

# 3. 각 branch checkout + amend + force-with-lease push
for b in phase-1a/day-10-followup phase-1a-followup/baseline-update \
         phase-1a-followup/plan-update phase-1b/week-3-plan; do
  git checkout "$b"
  git commit --amend -F /tmp/scp-pr<N>-v2.md
  git push --force-with-lease origin "$b"
done

# 4. gh pr edit --body-file 4개 + PR title scope 정정 (docs(portfolio/plan) → docs)
gh pr edit 60 --body-file /tmp/scp-pr60-v2-body.md --title "docs: ..."
# 동일 패턴 #61 #62 #63
```

![4 PR amend + force-with-lease push terminal 결과 (4 branch SHA 갱신)](./2026-05-14-day-10-pr-convention-regression/screenshots/02-amend-force-push-4-pr.png)

force push 결과 — 각 branch SHA 갱신:

| Branch | Before | After |
|---|---|---|
| phase-1a/day-10-followup | e7c3ee3 | 57a79e1 |
| phase-1a-followup/baseline-update | 16a60ce | 479bc17 |
| phase-1a-followup/plan-update | f7b8a92 | 37bbd4b |
| phase-1b/week-3-plan | f556c57 | 20acf9d |

![GitHub PR #60 commit history (force push 후 SHA 변경 흔적)](./2026-05-14-day-10-pr-convention-regression/screenshots/03-pr60-commit-history-force-push.png)

### 2.5. 학습 — PR template SoT 의무 + grep self-check 자체 추상화 + scope CONTRIBUTING.md SoT

학습 자산 3종:
1. **PR template SoT 의무**: `.github/PULL_REQUEST_TEMPLATE.md` 가 단일 출처. 메모리 컨벤션의 sub-header 매핑은 보조 참조. 매 PR 작성 시 template 실제 file 확인 의무 (memory 만 보고 작성 X).
2. **grep self-check 회귀 회피**: PR 본문 안 grep 명령 인용 시 매칭 keyword 포함 → 본문 자체가 회피 대상에 매칭. 추상화 정공 (메모리 컨벤션 SoT 인용 형태).
3. **scope CONTRIBUTING.md SoT 의무**: `kafka / flink / dbt / infra / producers / api / web / lakekeeper / postgres / scripts / cdc / iceberg / spark / cloudflare / env / slo` list 외 scope 사용 안 함. 본 list 외는 scope 생략.

본 사례 = 본 프로젝트 운영 패턴 중 가장 큰 자체 회귀 (4 PR amend + force push). 다음 PR 부터 회귀 회피 의무.

![PR #60 정정 전/후 commit body 비교 (template 위반 → 정공 구조 적용)](./2026-05-14-day-10-pr-convention-regression/screenshots/01-pr60-before-after-template.png)

---

## 3. Phase 1B 진입 시 인용 의무

본 archive 의 학습 자산은 Phase 1B Day 11-18 의 모든 PR + plan-update commit 작성 시점에 인용 의무:

- **§1 plan-update drift 패턴** — Phase 1B Week 3 plan (`phase-1b-week-3.md`) 의 Task 11.1-18.2 모두 골격만 박힘. 각 Day 진입 직전 plan-update commit 으로 상세 step 작성 + drift 발견 시 본 §1.4 의 sub-section 패턴 (plan SoT 본문 직접 정정 X + 새 sub-section 에 변경 본문 박음) 적용.
- **§2.1 PR template SoT 의무** — Phase 1B 의 모든 PR 작성 시점에 `.github/PULL_REQUEST_TEMPLATE.md` 실제 file 확인 의무. 메모리 sub-header 매핑은 보조 참조.
- **§2.2 jargon 회피** — Phase 1B 의 모든 PR / commit message 작성 시점에 본 §2.2 표 의 풀이 매핑 인용 의무. memory `korean-conventions` 갱신 (사용자 발화 SoT 2026-05-14 = baseline 정착 기술 용어 인정) 도 인용.
- **§2.3 grep self-check 회귀 회피** — PR 본문 안 grep 명령 인용 시 추상화 정공 (메모리 컨벤션 SoT 인용 형태).
- **§2.5 scope CONTRIBUTING.md SoT** — Phase 1B 의 새 PR scope (예: cloudflare / web / api / cdc) 채택 시 CONTRIBUTING.md scope list 안 단어만 사용. list 외 scope 사용 안 함 (생략 권장).

---

> **스크린샷 디렉토리** — 본 archive 본문 안 inline anchor 의 file 위치 = `./2026-05-14-day-10-pr-convention-regression/screenshots/<NN>-<short>.png` 패턴 (Day 9 archive SoT). 캡처 file 부재 시 broken image 표시 — 별도 PR 로 file 업로드 시점에 자동 정상화. 향후 강화 리포트 v2 (§p10 트러블슈팅 + §p11 운영 패턴 진화) 슬라이드 작성 시 reuse.
