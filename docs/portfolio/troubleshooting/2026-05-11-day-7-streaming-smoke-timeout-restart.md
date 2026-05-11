# Day 7 운영 후속 — b2s/s2g silent exit (SMOKE_RUN_SECONDS) + uv flink extra 누락 진단

> 작성: 2026-05-11 19:55 KST
> 영역: Day 7 PR γ (#43) 머지 직후 사전 점검에서 발견된 운영 issue. PR α/β/γ 코드 영역과 분리된 운영 후속.
> 관련 PR: 본 PR δ (docs only)
> 직전 archive: [`2026-05-11-day-7-nextjs-cloudflare-deploy.md`](2026-05-11-day-7-nextjs-cloudflare-deploy.md) (Day 7 PR α/β/γ 코드 영역)

## 0. 발견 흐름

Day 7 entry plan 종료 + at job 1 (SLO 24h 측정) 등록 후 사용자 사전 점검:

```bash
ps aux | grep -E "producers\.|flink_jobs\." | grep -v grep
# hotspot/subway producer 2 process 만 출력. bronze_to_silver / silver_to_gold 안 보임.

uv run python scripts/duckdb_check.py | tail -15
# silver_arrival_ts 마지막 = 2026-05-11 10:27:15 (본 세션 가동 직후만 처리, 약 9시간 정지)
# gold window_start = 2026-05-09 02:30 (이틀 전 데이터 그대로)
```

본 세션이 10:22 가동했고 약 9시간 후 (19:30 사전 점검 시점) 에서 **4 PID 중 streaming 2개만 silent stop** 식별. ERROR/Exception/Traceback grep 0 hit + last checkpoint commit 0 hit — Day 4 archive 의 "silent commit fail" 과 동일한 fingerprint 였으나 root cause 는 완전히 달랐다.

## 1. Issue 1 — `SMOKE_RUN_SECONDS=600` timeout (silent exit 1차)

### 증상

- b2s log 마지막 entry `2026-05-11 10:32:15` (가동 후 약 10분)
- s2g log 마지막 entry `2026-05-11 10:27:45` (가동 후 약 5분)
- 마지막 line 이 `Got brand-new decompressor [.gz]` 같은 정상 Hadoop CodecPool log
- ERROR / Exception / Traceback / LinkageError / OutOfMemory grep 모두 0 hit
- `committed snapshot` 같은 Iceberg commit log 도 0 hit

### 진단 (의도된 timeout 식별)

`bronze_to_silver.py:195~201` + `silver_to_gold.py:140~150` 의 main 함수:

```python
t_env.execute_sql(insert_sql)
log.info("Streaming job submitted")
log.info("Streaming 가동 중. SIGTERM 대기 (최대 %ds).", SMOKE_RUN_SECONDS)
time.sleep(SMOKE_RUN_SECONDS)
log.info("Smoke run timeout, exiting.")
```

`SMOKE_RUN_SECONDS` 환경변수:

```python
SMOKE_RUN_SECONDS = int(os.environ.get("FLINK_SMOKE_RUN_SECONDS", "600"))
```

default **600초 = 10분**. 변수명 `SMOKE_RUN_SECONDS` 자체가 "smoke run" (단발 검증) 용 설계 — `t_env.execute_sql(insert_sql)` 가 비동기 INSERT 등록 후 main 이 10분 동안 sleep → 10분 후 자연 종료. SLO 24h 측정 같은 long-running 시나리오에 부적합.

코드 주석 (b2s.py:196) 도 명시:
> "SIGTERM 받으면 즉시 종료. **운영 시점에는 별도 deploy mode (per-job cluster) 로 변경 검토**."

본 세션이 그 boundary 를 우연히 넘은 사용 시나리오 — 24h 측정 의도가 smoke run 설계와 충돌.

### 해결 (환경변수 우회)

```bash
export FLINK_SMOKE_RUN_SECONDS=90000  # 25시간 (24h + 1h 여유)
nohup uv run python -m flink_jobs.bronze_to_silver > /tmp/scp-day7-logs/b2s.log 2>&1 & disown
nohup uv run python -m flink_jobs.silver_to_gold > /tmp/scp-day7-logs/s2g.log 2>&1 & disown
```

환경변수만 조정해 코드 수정 없이 25h 가동 보장. SLO 24h 측정 후 1h 여유로 결과 안정성 보존.

### 한계 + Day 8 코드 수정 후보

`SMOKE_RUN_SECONDS=90000` 도 결국 25h 후 또 timeout. **24/7 운영 시점 (Phase 1B / Phase 2)** 에는 다음 코드 수정 필요:

```python
# 후보 1 — signal.pause() (SIGTERM 명시 종료)
import signal
signal.pause()  # SIGTERM / SIGINT 까지 대기

# 후보 2 — 무한 loop + alive log
while True:
    time.sleep(3600)
    log.info("alive (1h heartbeat)")

# 후보 3 — Day 4 archive 의 result.wait() 패턴 (mini-cluster 자체 대기)
result = t_env.execute_sql(insert_sql)
result.wait()
```

Day 8 진입 시 별도 hotfix PR (예: `phase-1a/day-8-streaming-long-running`) 으로 검토.

## 2. Issue 2 — uv dev/flink extra mutual exclusive (재가동 실패 2차)

### 증상

환경변수 우회 후 첫 재가동 시도:

```bash
nohup env FLINK_SMOKE_RUN_SECONDS=90000 uv run python -m flink_jobs.bronze_to_silver > /tmp/scp-day7-logs/b2s.log 2>&1 & disown
```

10초 후 ps 에 새 PID 안 잡힘. log 확인:

```
Traceback (most recent call last):
  File "/.../src/flink_jobs/bronze_to_silver.py", line 20, in <module>
    from pyflink.table import DataTypes, TableEnvironment
ModuleNotFoundError: No module named 'pyflink.table'
```

### 진단

`pyproject.toml` 확인:

```toml
[project.optional-dependencies]
# flink: PyFlink 1.20 의 transitive dep apache-beam 2.48 setup.py 가 pkg_resources +
# ... protobuf 버전 범위가
# local 에선 streaming 작업 = `--extra flink`, dbt 작업 = `--extra dbt` 로
flink = [
    "apache-flink==1.20.0",
    ...
]

[tool.uv]
default-groups = [...]
# flink (apache-beam 2.48) 와 dbt (dbt-core 1.9+) 의 protobuf 버전 범위가
# 호환 안 됨. 동시 install 불가.
```

**dev / flink / dbt extras 가 mutual exclusive** — protobuf 버전 범위 충돌로 동시 install 안 됨. `uv sync --extra X` 가 다른 extra 의 deps 를 제거.

본 세션 흐름:
1. **10:22 producer/streaming 초기 가동** — venv 에 flink extra 가 있던 상태. 정상 가동.
2. **PR α/β review 단계** — spec/code quality reviewer subagent 가 `uv sync --extra dev` (또는 `uv run pytest`) 실행 → venv 가 dev extra 로 전환 → pyflink 사라짐.
3. **19:45 재가동 시도** — venv 에 pyflink 없음 → ModuleNotFoundError.

즉 producer 는 dev extra 의 deps (`httpx`, `confluent-kafka` 등) 로 가동되어서 dev sync 후에도 그대로 살아있었고, streaming 만 pyflink 의존이라 죽음. 죽은 시점은 10:32 (SMOKE_RUN_SECONDS timeout) 였고, 그 후 pyflink 사라진 venv 위에 재가동 안 되는 상태 누적.

### 해결

```bash
uv sync --extra flink  # flink extra 복원, dev extra 의 deps 빠짐
uv run python -c "from pyflink.table import TableEnvironment; print('OK')"  # 검증
```

검증 통과 후 b2s/s2g 재가동 → 정상 startup (PID 48441 + 48459).

### Day 8/9 reviewer dispatch 시점에 같은 패턴 재발 가능

Day 8/9 의 reviewer subagent dispatch 마다 `uv sync --extra dev` 실행 → flink extra 사라짐 → streaming 죽으면 (또는 직후 새 가동 시도) `ModuleNotFoundError` 재발.

**Day 8 진입 시 의무 사항** — review 단계 종료 직후:

```bash
# review 끝나면 즉시 flink extra 복원 (streaming 4 PID 영속 가동 위해)
uv sync --extra flink

# 검증
uv run python -c "from pyflink.table import TableEnvironment; print('OK')"

# (옵션) streaming alive 확인
ps aux | grep -E "flink_jobs\." | grep -v grep | wc -l
# 2 이상 기대 (b2s + s2g)
```

또는 review subagent dispatch prompt 에 "venv 영구 수정 금지, `uv run --no-sync --extra dev` 또는 ephemeral 활성화로 한정" 명시 검토.

## 3. Issue 3 — caffeinate timer ↔ at job fire 시간 간격 의무

### 증상

본 세션이 24h timer caffeinate (PID 47551, 시작 19:33) 를 띄운 상태에서 b2s/s2g 재가동 (19:48) + at job 2 등록 (내일 19:45 fire).

- caffeinate 만료 = 19:33 + 24h = **내일 19:33**
- at job 2 fire = **내일 19:45**
- 간격 = caffeinate 종료 후 12분 동안 sleep 진입 가능 → atrun 의 fire 가 sleep 중에는 deferred → 19:45 정시 실행 안 됨

### 해결

```bash
kill 47551  # 24h timer 종료
nohup caffeinate -i -t 90000 > /tmp/caffeinate.log 2>&1 & disown  # 25h timer 신규
# 만료 = 19:49 + 25h = 다음 날 20:49 → at job 19:45 보다 1h 여유
```

### 운영 패턴

at job fire 시점 ± 1h 이상 caffeinate timer cover 의무. caffeinate `-t` 옵션의 second 단위 timer 는 자동 해제라 safe (kill 명시 안 해도 자동 종료).

| 시나리오 | caffeinate timer | at job fire | 안전? |
|---|---|---|---|
| 24h timer + 24h 후 at | 86400s | +24h 0m | ❌ 동시 종료, race condition |
| 25h timer + 24h 후 at | 90000s | +24h 0m | ✅ 1h 여유 |
| 25h timer + 23h 57m 후 at | 90000s | +23h 57m | ✅ 1h 3m 여유 (본 세션 실제 적용) |

## 4. Issue 4 — PyFlink mini-cluster 의 stdout commit log 누락 + duckdb_check grep 명령 정확성

### 증상

b2s/s2g 재가동 (19:48) 후 약 10분 가동된 시점에 사용자 사전 점검:

```bash
grep -E "snapshot|committed" /tmp/scp-day7-logs/b2s.log | tail -3
# 0 hit (빈 출력)
```

데이터는 실제로 commit 중 (silver_arrival_ts 19:52:35, gold window_start 19:00~19:20 fresh) 인데도 log keyword grep 결과 0 hit. 진단 명령으로 b2s log 전체 분석:

```bash
wc -l /tmp/scp-day7-logs/b2s.log
# 32 줄 (10분 가동 동안)

grep -iE "commit|snapshot|appended|written|checkpoint.*complete|writer|datafile|manifest" /tmp/scp-day7-logs/b2s.log
# 0 hit
```

→ stdout 에 출력되는 log 가 `org.apache.hadoop.io.compress.CodecPool` 의 `Got brand-new compressor/decompressor [.gz]` 만 32줄.

### 진단

**root cause**: PyFlink mini-cluster (`LocalEnvironment`) 의 default log4j 설정이 Iceberg / Flink checkpoint commit log 를 stdout 으로 routing 안 함. Iceberg commit 자체는 정상 (snapshot 누적 + row count 증가), 단 log 가 mini-cluster 내부에 stay (or `/tmp` 의 별도 file 로 분리되거나 silent).

phase-1a-progress 메모리 "Day 5 진입 전 lib 추출 (PR #28)" §학습 패턴에 이미 명시된 학습:

> "mini-cluster logger 가 stdout 에 안 가는 환경 — checkpoint commit 직접 검증은 Iceberg snapshot count 또는 row count 변화로"

본 사건이 그 학습의 실 적용 사례. Day 4 archive 의 ClassLoader fix 시 `restart-strategy=none + result.wait()` 패턴이 silent fail 진단의 임계점이었던 이유도 동일 — log 만 봐서는 commit 실 발생 여부 알 수 없으니 직접 wait 강제 + stack trace 명시 노출 필요.

### 부수 발견 — duckdb_check.py grep 명령의 정확성

본 issue 진단 직후 추가 grep 시도:

```bash
# ❌ silver_arrival_ts 는 column 이름 — 출력 안 보임
uv run python scripts/duckdb_check.py | grep "silver_arrival_ts"
# 0 hit

# ❌ bronze count 헤더만 잡고 다음 줄 (count 숫자) 안 보임
uv run python scripts/duckdb_check.py | grep "bronze.hotspot_raw count"
# == bronze.hotspot_raw count == (만 출력)

# ✅ section header + -A N 으로 row 까지 같이 잡음
uv run python scripts/duckdb_check.py | grep -A 3 "gold.fact_hotspot"
# == gold.fact_hotspot_congestion_5min sample ==
# (datetime(...), district, ...) × 3 row 정상 출력
```

`duckdb_check.py` 의 출력 구조 = `== section header ==` 다음에 row data 가 `datetime.datetime(...)` 같은 raw repr 로 출력. column 이름은 출력 안 됨. → **grep 으로 column name 직접 검색 불가**.

### 해결 — 정확한 검증 명령 inventory

```bash
# 방법 1 (권장) — 전체 출력 보고 사람이 판단
uv run python scripts/duckdb_check.py | tail -25

# 방법 2 — section 별 -A N 추가
uv run python scripts/duckdb_check.py | grep -A 1 "bronze.hotspot_raw count"
uv run python scripts/duckdb_check.py | grep -A 3 "silver.hotspot_congestion sample"
uv run python scripts/duckdb_check.py | grep -A 3 "gold.fact_hotspot_congestion_5min sample"
uv run python scripts/duckdb_check.py | grep -A 3 "district 별 latest"

# 방법 3 — gold window_start 가 fresh 인지 직접 판단 기준
# 현재 시간 - window_start <= 10분 이면 정상 streaming
# 현재 시간 - window_start > 1시간 이면 s2g 죽음 의심
```

### 학습 (Day 4 archive reuse 확장)

- **mini-cluster logger silent 가 본 프로젝트 기본 가정** — 모든 streaming 검증의 default path 는 log keyword X, row count + snapshot count + gold window_start fresh ✅
- **duckdb_check.py 의 grep 한계** — column name 검색 X, section header + `-A N` ✅
- **본 archive 의 학습이 future 자산이 되는 정공** — 본 issue 가 사용자 사전 점검 직후 즉시 발견. archive 보강이 같은 의문 future 재발 방지

## 5. 운영 final state (사용자 검증용)

본 세션 마무리 시점 (2026-05-11 19:50 KST) 의 모든 자원:

| 자원 | PID / 큐 | timer / fire |
|---|---|---|
| hotspot_producer (wrapper / child) | 32598 / 32606 | 5분 polling, 영구 |
| subway_producer | 32599 / 32605 | 1분 polling, 영구 |
| bronze_to_silver (재가동) | 48437 / 48441 | 25h (90000s), 만료 ~ 2026-05-12 20:48 |
| silver_to_gold (재가동) | 48457 / 48459 | 25h, 만료 ~ 2026-05-12 20:48 |
| caffeinate (sleep 방지) | 48702 | 25h, 만료 ~ 2026-05-12 20:49 |
| atrun daemon | (system) | 영구 활성화 (sudo launchctl load) |
| at job 2 (SLO 측정) | 큐 | 2026-05-12 19:45 fire |

내일 19:45 측정 결과: `/tmp/slo-24h-<timestamp>.log` (FreshnessReport count / p50 / p95 / p99 / max / slo_violated).

## 6. 학습 패턴 5종

### 6-1. silent exit 의 root cause 분류 (Day 4 archive 확장)

Day 4 Task 1 의 "silent commit fail" 은 ClassLoader LinkageError (실 fail). 본 사건은 의도된 `time.sleep(N)` timeout (자연 종료). 둘 다 ERROR/Exception log 가 없어 fingerprint 동일하지만 root cause 정 반대:

| 패턴 | Day 4 (LinkageError) | Day 7 (SMOKE timeout) |
|---|---|---|
| log 마지막 line | `prepareSnapshotPreBarrier` 직후 (commit step 도달 못함) | `Got brand-new decompressor` 같은 정상 동작 log |
| process 상태 | crash exit (return code !=0) | normal exit (return code 0) |
| 데이터 commit | 0건 (snapshot 0) | 가동 동안 정상 commit 누적 후 종료 |
| 해결 | code fix (ClassLoader parent-first) | env var 우회 또는 code fix (signal.pause()) |

**진단 우선순위**:
1. log 마지막 timestamp 가 `SMOKE_RUN_SECONDS` 와 일치하는가? → Issue 1
2. log 에 stack trace 가 있는가? → Day 4 패턴
3. 가동 동안 Iceberg snapshot commit 이 발생했는가? → snapshot 0 면 client side fail, 정상 commit 있으면 SMOKE timeout

### 6-2. uv extra mutual exclusive 의 venv 영구 영향

dev / flink / dbt 가 같은 venv 에 동시 install 안 됨 (pyproject.toml 의 [tool.uv] default-groups + protobuf 충돌 주석). reviewer subagent 가 dev sync 하면 streaming 의존 (pyflink) 이 venv 에서 빠짐 → 후속 streaming 가동 시 ModuleNotFoundError.

**우회 패턴**:
- review 직후 `uv sync --extra flink` 즉시 복원
- 또는 reviewer prompt 에 "venv 영구 수정 금지, ephemeral 활성화" 명시
- 또는 코드 수정 — flink/dev 가 같은 venv 에 install 되도록 protobuf 충돌 해결 (Phase 2 영역, 본격 운영 시점)

### 6-3. SMOKE_RUN_SECONDS 같은 design boundary 의 인지 의무

`bronze_to_silver.py:196` 주석:
> "운영 시점에는 별도 deploy mode (per-job cluster) 로 변경 검토."

코드 작성 시 design boundary 가 명시되어 있으나, 본 세션의 "24h SLO 측정" 의도가 그 boundary 를 넘는 사용 시나리오 → 환경변수 우회 → 잠시 보존되었으나 본격 운영 (Phase 1B / Phase 2) 시점에는 code fix 필수.

**운영 boundary 추적 원칙** — 코드 주석 / docstring 의 "검토" / "임시" / "smoke" / "TODO" 표현은 future 자산. Day 8 hotfix PR 의 후보 inventory 명시 의무.

### 6-4. caffeinate ↔ at job 간격 의무 (운영 timing)

본 세션의 첫 caffeinate timer (24h) + at job (정확 +24h) = race condition. 1h 여유 timer 가 safe path. atrun daemon 의 fire 는 sleep 중 deferred 라 정확 시각 보장이 caffeinate 의 sleep 차단에 의존.

**운영 timer 의 분리 가능성**:
- caffeinate `-t` 옵션 = 자동 해제 (kill 불요)
- 여러 caffeinate 중첩 가능 (effect 동일, max timer 까지 cover)
- `at job` fire 직후 caffeinate 의 자연 해제는 무관 (측정 완료 후 sleep 진입해도 영향 없음)

### 6-5. mini-cluster log keyword 검증의 한계 + 정확한 검증 패턴 (Day 4 archive 학습 reuse)

PyFlink mini-cluster 의 stdout 에 Iceberg / Flink commit log 가 routing 안 됨 (§4 Issue 4 SoT). `grep snapshot|committed` 같은 keyword 검증은 본 환경에서 항상 0 hit — log 만 봐서는 commit 실 발생 여부 판단 불가.

**streaming commit 검증의 default path** (Day 8 이후 모든 streaming 작업에 reuse 의무):

1. **gold window_start fresh** — 현재 시간 vs window_start 차이 ≤ 10분 이면 정상. > 1시간 이면 s2g 죽음 의심
2. **silver row count 변화** — 5분 후 다시 측정해서 row 증가하면 정상
3. **bronze count 누적 추이** — producer alive 검증 (5분 polling 마다 +3~9 row)
4. **process alive (ps aux)** — wrapper PID + child PID 둘 다 살아있는지

**duckdb_check.py 의 grep 명령 정확성** (Issue 4 SoT):

```bash
# ❌ column name 직접 검색 X
grep "silver_arrival_ts"  # 0 hit (column 이름이라 출력 안 보임)

# ✅ section header + -A N
grep -A 1 "bronze.hotspot_raw count"
grep -A 3 "silver.hotspot_congestion sample"
grep -A 3 "gold.fact_hotspot_congestion_5min sample"

# ✅ 가장 정공 — 전체 출력 보고 사람이 판단
uv run python scripts/duckdb_check.py | tail -25
```

본 학습은 phase-1a-progress 메모리 "Day 5 진입 전 lib 추출 §학습 패턴" 의 mini-cluster logger SoT 를 archive 본문으로 명문화한 사례. Day 6 학습 패턴 5 (archive = future 자산) 의 정공 적용.

## 7. Day 8 진입 시 의무 사항 (운영 후속 inventory)

본 archive 의 학습을 Day 8 새 세션에서 reuse 의무:

1. **SMOKE_RUN_SECONDS code fix 검토** — `signal.pause()` 또는 무한 loop + heartbeat. 별도 hotfix PR 또는 Day 9~10 cleanup pass.
2. **review subagent 직후 flink extra 복원** — `uv sync --extra flink` 자동화. reviewer dispatch prompt 보강 검토.
3. **at job + caffeinate timer 분리** — Day 8 진입 시 새 작업이 또 streaming 4 PID 의존하면 동일 패턴 재발 가능.
4. **process alive 의 주기 모니터링** — 매 1~6시간 ps 확인 또는 launchd watchdog 도입 검토 (Phase 1B / Phase 2 영역).
5. **streaming 검증 = log keyword X, row count + gold window_start fresh ✅** (§4 + §6-5 SoT). 모든 streaming 가동 후 검증 시 `grep snapshot|committed` 같은 log keyword 안 쓰고 `duckdb_check.py | tail -25` 의 row data 또는 `gold window_start` fresh 여부로 판단. Day 4 archive 학습의 default path.

## 8. 관련 문서

- 직전 archive (Day 7 PR α/β/γ 코드 학습): [`2026-05-11-day-7-nextjs-cloudflare-deploy.md`](2026-05-11-day-7-nextjs-cloudflare-deploy.md)
- Day 4 archive (silent commit fail, ClassLoader fix): [`2026-05-08-day-4-silver-fix-resolved.md`](2026-05-08-day-4-silver-fix-resolved.md) + [`2026-05-07-day-3-task-3.4-silver-debug.md`](2026-05-07-day-3-task-3.4-silver-debug.md)
- Day 4 Task 4.1~4.3: [`2026-05-09-day-4-tasks-4_1-4_3.md`](2026-05-09-day-4-tasks-4_1-4_3.md) (lib/duckdb_iceberg 우회 패턴 SoT)
- Day 6 archive (5종 학습 패턴): [`2026-05-11-day-6-airflow-cdc-integration.md`](2026-05-11-day-6-airflow-cdc-integration.md)
- spec §6-1 Day 7 / §9-1 Day 7 fallback
- `bronze_to_silver.py:195~201` + `silver_to_gold.py:140~150` (SMOKE_RUN_SECONDS 패턴 SoT)
- `pyproject.toml [project.optional-dependencies]` + `[tool.uv]` (dev/flink/dbt mutual exclusive 주석 SoT)
