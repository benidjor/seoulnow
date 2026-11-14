# VM 메모리 고갈 · 재시작마다 Kafka 7일치 재처리

**발생일**: 2026-11-11 – 11-14 (확인 2026-11-15)
**환경**: Oracle ARM VM (24GB · 4코어 · 스왑 없음) · systemd 호스트 프로세스 · PyFlink 1.20 로컬 실행 · Iceberg 1.7.1

## 요약

같은 VM 에서 도는 다른 파이프라인이 2026-11-11 에 네 번 멈췄는데 원인은 VM 메모리 고갈이었다.
메모리를 가장 많이 쓴 프로세스는 seoulnow 의 Flink 잡 두 개였다.
대응안을 검토하면서 VM 을 읽기 전용으로 조사하다가 더 큰 문제를 찾았다.
Flink 잡이 재시작할 때마다 Kafka 에 남은 7일치 메시지를 처음부터 다시 읽어 bronze · silver 에 중복으로 넣고 있었다.
5월 이후 이런 재처리가 13번 있었고 bronze 행의 약 42% 가 중복이다.
재시작을 일으킨 것은 배포가 아니라 Ubuntu 자동 업데이트였다.

| 이슈 | 상태 |
| --- | --- |
| 1. 재시작마다 Kafka 7일치 재처리 → 중복 | 원인 확정 · 이 PR 에서 수정 |
| 2. Flink JVM 에 메모리 한도 없음 | 원인 확정 · 후속 PR 에서 한도 적용 |
| 3. 11-12 – 11-14 약 53시간 커밋 중단 | 원인 미확정 |

## Issue 1 — 재시작마다 Kafka 7일치를 다시 읽는다

### 증상

Iceberg 스냅숏 요약의 `added-records` 를 보면, 평소 bronze 에는 하루 864행 (핫스팟 3곳 × 5분 간격 288회)이 들어온다.
재시작 직후에는 스냅숏 하나에 6천 – 1만 1천 행이 한꺼번에 들어온다.

| 재시작 | bronze 한 스냅숏 행 수 | silver | gold |
| --- | --- | --- | --- |
| 2026-11-11 06:13 | 6,352 | 같음 | 3 |
| 2026-11-14 06:09 | 7,000 | 같음 | 2,331 |
| 2026-11-15 07:53 (배포) | 7,929 | 같음 | 3 |

- bronze 에서 1,000행이 넘는 스냅숏은 2026-05-29 이후 13개이고 합은 104,301행이다.
- bronze 전체는 250,117행이므로 약 42% 가 재처리로 들어온 중복이다.
- gold 에도 1,000행이 넘는 스냅숏이 2개 (07-20 · 11-14)이고 합은 5,959행이다.
- 11-15 07:53 에 다시 들어온 7,929행은 Kafka 토픽에 보존된 메시지 수 (earliest offset 부터 latest 까지 약 7,930건)와 거의 같다.

### 원인

Kafka 소스는 `'scan.startup.mode' = 'earliest-offset'` 으로 설정돼 있었고 체크포인트 저장 위치 (`state.checkpoints.dir`)는 지정되지 않았다.
체크포인트가 프로세스 메모리에만 있으므로 재시작하면 복원할 상태가 없다.
그래서 잡은 매번 토픽의 가장 오래된 offset 부터 읽고, 보존 기간 (`retention.ms` = 7일) 동안 쌓인 메시지가 전부 다시 들어온다.

- 체크포인트 때 커밋한 consumer group offset 은 브로커에 정상적으로 남아 있었다 (`flink-bronze-hotspot` 의 lag 0).
  `earliest-offset` 모드는 이 값을 쓰지 않을 뿐이다.
- gold 의 중복은 재시작마다 생기지는 않는다.
  두 잡이 거의 동시에 재시작해 silver 의 재처리 데이터가 watermark 보다 먼저 도착하면 과거 window 가 다시 계산된다 (11-14).
  watermark 가 이미 앞서 있으면 늦게 도착한 데이터로 처리돼 버려진다 (11-11 · 11-15).

**재시작을 일으킨 주체**: 13번 가운데 11번이 06시대였다 (나머지는 07-20 11:48 · 11-15 07:55 배포).
11-14 기록을 보면 06:08:43 에 `unattended-upgrade` 가 돌기 시작했고 06:08:45 에 `apt-daily-upgrade` 가 systemd 를 다시 실행했다.
그 직후 06:08:58 – 06:09:05 사이에 seoulnow-api · seoulnow-bronze-silver 가 재시작됐다.
자동 업데이트가 라이브러리를 갱신하면서 그 라이브러리를 쓰는 서비스를 재시작했다.
즉 배포가 없어도 며칠에 한 번씩 재시작이 일어난다.

**기각한 가설**: 검토 단계에서 Iceberg 스트리밍 소스 (bronze → silver · silver → gold)도 재시작할 때 테이블 전체를 다시 읽는다고 추정했다.
실측해 보니 silver 에 들어온 행 수가 bronze 에 다시 들어온 행 수와 정확히 같았으므로, Iceberg 소스는 최신 스냅숏 이후만 읽은 것으로 보고 이 가설을 버렸다.

### 해결

Kafka 소스의 시작 위치를 커밋된 group offset 으로 바꿨다 (`src/flink_jobs/bronze_to_silver.py`).

```sql
'scan.startup.mode' = 'group-offsets',
'properties.auto.offset.reset' = 'earliest',   -- 커밋된 offset 이 없을 때 (첫 기동)만
```

- Flink Kafka 소스는 체크포인트가 끝날 때 offset 을 커밋하므로, 재시작하면 마지막 체크포인트 지점부터 이어 읽는다.
- 마지막 체크포인트 (30초 주기)와 종료 사이에 들어온 메시지는 한 번 더 들어올 수 있다.
  체크포인트를 디스크에 보존하고 그 지점에서 복원하는 방법이 더 정확하지만 시작할 때마다 최신 체크포인트 경로를 찾는 코드가 필요해 이번에는 쓰지 않았다.
- 이미 쌓인 중복 (bronze · silver 약 10만 행 · gold 약 6천 행)은 이 변경으로 지워지지 않는다.
  compaction 과 area_code 정정 작업에서 데이터를 다시 적재할 때 함께 정리한다.

## Issue 2 — Flink JVM 에 메모리 한도가 없다

### 증상

같은 VM 의 다른 파이프라인이 멈춘 2026-11-12 00:48 에는 java 프로세스 RSS 합이 약 11GB 였다.
11-15 07:53 에 재시작한 뒤 5분 동안 cgroup 의 `anon` 메모리를 15초 간격으로 재면 다음과 같다.

| 시각 | bronze-silver | silver-gold | VM available |
| --- | --- | --- | --- |
| 07:54:59 | 2,932MB | 1,876MB | 7,889MB |
| 07:56:59 | 5,464MB | 3,602MB | 6,009MB |
| 07:58:45 | 5,745MB | 4,534MB | 4,626MB |

### 원인

유닛에 `MemoryMax` 가 없고 JVM 인자에 `-Xmx` 도 없어, JDK 17 기본값인 물리 메모리의 1/4 (`MaxHeapSize` = 6,287,261,696 바이트)이 최대 힙이 된다.
G1 은 한 번 늘린 힙을 운영체제에 잘 돌려주지 않는다.

| 항목 (07:59 기준) | bronze-silver | silver-gold |
| --- | --- | --- |
| 힙 확보 (committed) | 5,588MB | 3,936MB |
| 힙 사용 (used · 쓰레기 포함 · full GC 0회) | 1,844MB | 1,946MB |
| Metaspace 사용 | 204MB | 177MB |
| 스레드 | 116 | 94 |

- 실제로 쓰는 힙은 2GB 안팎인데, 확보한 힙이 그 2 – 3배라 RSS 가 커졌다.
- 시작 직후 메모리가 급증한 데에는 Issue 1 의 재처리도 겹친 것으로 본다 (추정).

### 해결 (후속 PR)

측정값을 근거로 두 유닛에 `-Xmx3g` · `MaxMetaspaceSize=512m` · `MemoryHigh=4500M` · `MemoryMax=5G` 를 건다.
처음 검토한 안 (`-Xmx2g` · `MaxMetaspaceSize=256m` · `MemoryMax=4G`)은 위 측정값보다 작아서 시작하자마자 한도에 닿아 재시작을 반복했을 것이다.
Issue 1 을 먼저 고친 이유도 같다.
재처리가 남아 있으면 한도 때문에 재시작이 늘고 재시작할 때마다 중복도 늘어난다.

## Issue 3 — 약 53시간 동안 커밋이 없었다

### 증상

bronze · gold 두 테이블 모두 11-12 에 스냅숏이 58개 (평소 약 290개)만 생겼고 11-13 에는 하나도 없다.
11-14 06:09 재시작 뒤에야 다시 커밋되기 시작했다.
그동안 systemd 는 서비스를 `active` 로 보고 있었다.

### 원인 (미확정)

11-12 00:48 bronze-silver 로그에 Python UDF 연결 실패 (`failed to connect to all addresses` · `Connection refused`)가 남았다.
같은 시각 OOM killer 가 동작했으므로, 메모리 부족으로 UDF 실행 경로가 끊긴 뒤 잡이 진행하지 못한 것으로 추정한다.
프로세스가 살아 있어 `Restart=always` 가 동작하지 않았다.

### 남은 일

- 최근 커밋 시각을 보고 일정 시간 커밋이 없으면 경고하거나 재시작하는 감시 장치가 필요하다.
- 메모리 한도 적용 뒤 같은 증상이 다시 나오는지 지켜본다.

## 교훈

- **체크포인트가 메모리에만 있으면 `earliest-offset` 은 재시작마다 전체 재처리를 뜻한다.**
  streaming 잡의 시작 위치는 「처음 띄울 때」와 「다시 띄울 때」를 나눠 정해야 한다.
- **재시작은 배포 때만 일어나지 않는다.**
  자동 업데이트 · OOM · 수동 조작 모두 재시작을 일으키므로, 잡은 언제 재시작해도 결과가 같아야 한다.
- **메모리 한도는 측정한 뒤에 정한다.**
  물리 메모리를 나눠 잡은 예산만으로 정한 숫자는 실제 사용량보다 작았다.
- **`active` 는 정상을 뜻하지 않는다.**
  프로세스가 살아 있는지가 아니라 데이터가 계속 들어오는지를 봐야 한다.

## 조사 방법

조사는 모두 읽기 전용으로 했다.
데이터 파일을 읽어 중복을 확인하는 조회는 bronze 의 파일 목록을 만드는 단계에서 10분을 넘겨 중단했다 (파일 14.6만 개 · pyiceberg 순수 파이썬 Avro 디코더).
그래서 스냅숏 요약 (`added-records` · `total-records` · `total-data-files`)만 읽어 판단했다.

| 확인 | 명령 · 출처 |
| --- | --- |
| cgroup 메모리 | `/sys/fs/cgroup/system.slice/<유닛>.service/memory.stat` 의 `anon` · `memory.events` |
| JVM 힙 · metaspace | `jcmd <pid> GC.heap_info` · `jstat -gc <pid>` · `jcmd <pid> VM.flags` |
| Kafka 보존 · offset | `kafka-configs.sh --describe --all` · `kafka-get-offsets.sh --time -2 / -1` · `kafka-consumer-groups.sh --describe` |
| 재시작 주체 | `journalctl` (재시작 직전 1분) · `/var/log/apt/history.log` |
| 재처리 · 중복 | pyiceberg `table.snapshots()` 의 `summary` |
