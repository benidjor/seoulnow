# Day 11 backend 3-트랙 병렬 worktree 실행

> 작성: 2026-05-25 KST
> 시점: Day 11 Task 11.1-A / 11.2 / 11.3 을 3개 worktree 로 병렬 구현 후 통합 머지(PR #74/#75) 완료.
> 관련 메모리: `parallel-worktree-sessions` (워크플로우 SoT), `execution-policy` (단일 세션 subagent 패턴)

## 0. 요약

Day 11 backend 작업을 파일 disjoint 3-트랙으로 분해해 git worktree 로 동시 진행. 단일 세션 subagent 순차의 처리량 한계를 병렬로 극복하고, 충돌 0으로 통합.

## 1. 동기 + 제약

### 1.1. 동기

단일 세션 subagent-driven 은 task 를 순차 실행해 처리량이 제한됨. 사용자가 여러 세션 동시 진행을 원함.

### 1.2. 핵심 제약 — 충돌 원인은 Day 가 아니라 공유 디렉토리

같은 작업 디렉토리에서 두 세션을 띄우면 git working tree + index 를 공유해 한 세션의 commit / checkout 이 다른 세션을 깨뜨림.

- 다른 Day / Task 여도 같은 폴더면 충돌. 충돌 원인은 작업 단위가 아니라 공유 디렉토리.
- 해결 = `git worktree` 로 디렉토리 분리. 각 worktree 가 독립 working tree + 브랜치.

## 2. 분할 — 파일 disjoint 3-트랙

Task 11.1-A + 11.2/11.3 을 건드리는 파일이 겹치지 않게 3-트랙으로 분해.

- **Track A** (메인 디렉토리, `phase-1b/day-11-task-11.1a-backend`): `airflow/` + `infra/spark/` + 정찰 doc. gold compaction + DAG + 라이브 실측.
- **Track B** (worktree `../seoul-citydata-fastapi`): `src/api/` + `scripts/` + `tests/unit/api/`. FastAPI thread-safety + run_api.sh + 동시성 테스트.
- **Track C** (worktree `../seoul-citydata-ingestion`): `infra/kafka/` + `infra/http-receiver/` + `docker-compose.yml`. user.events.v1 토픽 + HTTP receiver.

유일한 공유 디렉토리 `tests/integration/` 은 파일명 분리(B=`test_api_concurrency.py`, C=`test_http_receiver.py`)로 충돌 회피.

## 3. 실행

### 3.1. worktree 셋업

```bash
git worktree add -b phase-1b/day-11-task-11.1a-fastapi ../seoul-citydata-fastapi phase-1b/day-11-task-11.1a-backend
git worktree add -b phase-1b/day-11-task-11.2-11.3-ingestion ../seoul-citydata-ingestion main
# 각 worktree 는 .venv 미공유 → uv sync 선행 (백그라운드로 미리 돌리면 새 세션 즉시 시작)
```

- Track B 는 backend HEAD 기반(spec/plan 공유, 나중에 backend 로 머지 = 한 PR).
- Track C 는 main 기반(11.1-A 와 독립 = 별도 PR).

### 3.2. 트랙별 kickoff 프롬프트

각 새 세션에 self-contained 프롬프트 제공.

- worktree 경로 + 브랜치 + 읽을 spec/plan 경로.
- 담당 task + 범위 밖 파일 명시("절대 건드리지 마라").
- 환경(`.venv/bin/python`, 공유 docker 스택은 읽기/additive 만).
- "머지/PR 은 메인 세션이" — 통합은 한 곳에서.

## 4. 수렴

### 4.1. 통합 머지

Track B 완료 후 `...fastapi` → `...backend` 로컬 머지(파일 disjoint 라 clean) → 전체 최종 리뷰 → PR #74.

- Track C 는 독립 PR #75.
- 통합 후 compaction 효과 + thread-safety 결합 재측정: 16 병렬 22.58s → 5.73s 전부 200.

### 4.2. 정리

머지 후 worktree 2개 제거 + 머지 브랜치 3개(로컬+원격) 삭제. 메인 디렉토리만 잔존.

## 5. cross-Day 병렬의 한계

다른 Day 작업의 병렬은 제한적. Phase 1B 가 순차 파이프라인이기 때문.

- 이벤트 인입 사슬: 11.3 토픽 → 11.2 receiver → 11.1-B Edge API → 14 PyFlink.
- 프론트 UX 사슬: 11.0 지도 → 12 북마크 → 13 푸시 → 15 회원가입.
- 뒤 Day 를 당기면 통합검증 불가 + 재작업 위험. 병렬 후보 = 독립 leaf(계약/스키마, dbt mart, infra) 한정.
- 본 사례가 가능했던 이유: 11.1-A(compaction/FastAPI) 와 11.2/11.3(인입)이 서로 독립 + 파일 disjoint.

## 6. 학습

다음 병렬 작업에 재사용할 원칙.

- 병렬 멀티세션은 worktree 디렉토리 분리 필수(같은 폴더 = git 충돌). Day / Task 분리만으로는 불충분.
- 분할 기준 = 파일 disjoint 그룹. 공유 파일은 파일명/경로로 분리.
- 각 트랙 self-contained kickoff + 범위 밖 명시 → 트랙 간 침범 0.
- 공유 docker 스택은 additive(신규 토픽/마트) + Iceberg snapshot 격리라 동시 read/write 안전.
- 통합은 한 세션(메인)에서 머지 + 최종 리뷰 + PR — 트랙 세션은 보고만.
