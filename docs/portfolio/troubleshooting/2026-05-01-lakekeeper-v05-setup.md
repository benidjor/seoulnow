# Lakekeeper v0.5 셋업에서 마주친 4가지 이슈

**발생일**: 2026-05-01 (Phase 1A Day 1)
**관련 plan**: `docs/superpowers/plans/phase-1a-week-1.md` Task 1.2 / 1.5
**관련 spec**: `docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md` §3, §5-4
**관련 commits**: `a340d89`, `5e6ceb0`, `04cd719`, `7b79c3d`

## 요약

`docker-compose.yml` 에 Lakekeeper REST Catalog v0.5 를 띄우고 `infra/lakekeeper/bootstrap.py` 로 `seoul` warehouse 를 등록하기까지, plan 본문이 가정했던 동작과 v0.5.2 의 실제 runtime 동작 사이에 4가지 차이를 발견했습니다. 모두 spec / docs 와 reality 의 gap 으로, **OpenAPI 라이브 스키마 + 컨테이너 직접 inspect** 를 통해 진단하고 fix 했습니다. plan 본문의 §9-1 fallback 트리거 (Lakekeeper 디버깅 2시간 초과 → JdbcCatalog 우회) 를 발동하지 않고 정상 경로로 closure 했습니다.

| # | 이슈 | 진단 시간 | 해결 commit |
|---|---|---|---|
| 1 | Schema migration 이 자동 실행되지 않음 | ~10분 | `a340d89` |
| 2 | Distroless 이미지라 healthcheck command 불가 | ~5분 | `a340d89` |
| 3 | Lakekeeper 가 docker network 안에서 MinIO 접근 (localhost X) | ~15분 | `5e6ceb0`, `04cd719` |
| 4 | Fresh instance 는 server bootstrap 필요 | ~5분 | `5e6ceb0` |

bonus 이슈: error response 형식 가드 누락 (review 가 발견, `7b79c3d` 로 fix).

---

## Issue 1 — Schema migration 이 자동 실행되지 않음

### 증상
첫 `docker compose up -d` 직후 lakekeeper 컨테이너가 즉시 exit code 1 로 죽음. 다른 4개 서비스(kafka / postgres / minio / minio-bootstrap) 는 정상 healthy.

### 원인 (logs 진단)
```
Lakekeeper Version: 0.5.2
Starting server on 0.0.0.0:8181...
Error: Error fetching bootstrap data
error returned from database: relation "server" does not exist
```

`server` 테이블이 Postgres 에 존재하지 않음 — Lakekeeper 는 첫 기동 시 schema migration 을 **자동으로 돌리지 않고 별도 명령** 으로 분리해 두었음. plan 본문의 `command: ["serve"]` 만으로는 부족.

`docker run --rm quay.io/lakekeeper/catalog:v0.5 --help` 출력에서 확인:
```
Commands:
  migrate             Migrate the database
  wait-for-db         Wait for the database to be up and migrated
  serve               Run the server - The database must be migrated before running the server
  healthcheck         Check the health of the server
  ...
```

`serve` subcommand 의 설명에 명시적으로 "The database must be migrated before running the server" — operator 책임.

### 해결
docker-compose 에 `lakekeeper-migrate` init container 추가. `command: ["migrate"]` + `restart: "no"` 로 한 번 돌고 종료. 메인 lakekeeper 서비스는 `depends_on.lakekeeper-migrate.condition: service_completed_successfully` 로 게이트.

```yaml
lakekeeper-migrate:
  image: quay.io/lakekeeper/catalog:v0.5
  ...
  command: ["migrate"]
  restart: "no"

lakekeeper:
  ...
  depends_on:
    postgres: {condition: service_healthy}
    minio-bootstrap: {condition: service_completed_successfully}
    lakekeeper-migrate: {condition: service_completed_successfully}
  command: ["serve"]
```

### 검증
재기동 시 lakekeeper-migrate logs 마지막 줄: `Database migration complete.` 이후 lakekeeper serve 가 정상 healthy.

---

## Issue 2 — Distroless 이미지라 plan 의 curl 기반 healthcheck 불가

### 증상
Issue 1 fix 후 lakekeeper 가 `serve` 까지 진행하지만 `docker compose ps` 에서 영원히 `(health: starting)`. host 측 `curl http://localhost:8181/health` 는 정상 응답.

### 원인
plan 본문의 healthcheck:
```yaml
test: ["CMD-SHELL", "curl -sf http://localhost:8181/health || exit 1"]
```

Lakekeeper 이미지는 distroless (`/home/nonroot/iceberg-catalog` 외에 거의 없음) — 컨테이너 안에 `curl`, `sh`, `wget`, `nc` 모두 없음. `CMD-SHELL` 자체가 `/bin/sh` 가 없어 실패.

```
docker run --rm --entrypoint /bin/sh quay.io/lakekeeper/catalog:v0.5 -c "command -v curl"
> stat /bin/sh: no such file or directory
```

### 해결
`--help` 에서 발견한 `healthcheck` subcommand 사용. binary 직접 호출 (`CMD` 형식, shell 없이):

```yaml
test: ["CMD", "/home/nonroot/iceberg-catalog", "healthcheck", "-s"]
```

`-s` 플래그는 health endpoint 만 (DB 는 별도 옵션). 빠르고 외부 의존성 없음.

### 검증
재기동 후 `docker compose ps` 에 `(healthy)` 표시.

### 교훈
Distroless / scratch 기반 이미지가 흔해지는 만큼, `CMD-SHELL + curl` 패턴은 점점 안 통함. 컨테이너 자체가 health subcommand 를 제공하는지 먼저 확인 (`--help`, `man`, 공식 docs).

---

## Issue 3 — Lakekeeper container 가 docker network 안에서 MinIO 에 접근

### 증상
Bootstrap.py 로 warehouse 등록 시 HTTP 424 (FailedDependency) 또는 storage validation 단계에서 실패. plan 본문의 default `MINIO_ENDPOINT=http://localhost:9000` 이 원인.

### 원인
Lakekeeper 는 warehouse 등록 시 **storage-profile 의 endpoint 로 실 S3 op 을 시도해 유효성 검증** 을 함 (file delete probe). Lakekeeper 컨테이너 입장에서 `localhost:9000` 은 자기 자신 (8181 포트 listener) — MinIO 로 도달 불가.

docker network 안에서는 service 이름이 hostname:
```
docker network inspect scp_default
# scp-lakekeeper, scp-minio 같은 네트워크
# 컨테이너 입장 hostname = compose service 이름 (minio)
```

→ Lakekeeper 컨테이너에선 `http://minio:9000` 으로 접근해야 함.

### 해결
1. `bootstrap.py` 의 `MINIO_ENDPOINT` default 를 `http://localhost:9000` → `http://minio:9000` 으로 변경.
2. host 측 클라이언트 (Python producer / PyFlink / Spark) 는 여전히 `localhost:9000` 을 사용해야 하므로 `.env.example` 의 `MINIO_ENDPOINT` 는 그대로 유지.
3. `.env.example` 의 `MINIO_ENDPOINT` 옆에 5줄 주석 추가: "이 변수를 export 한 채 bootstrap.py 를 실행하면 default 가 override 되어 잘못된 endpoint 가 등록되니 주의" — 운영 foot-gun 명시.

### 잠재 개선 (Phase 2 또는 후속)
환경변수 분리:
- `MINIO_ENDPOINT_HOST=http://localhost:9000` (host 측 클라이언트)
- `MINIO_ENDPOINT_INTERNAL=http://minio:9000` (Lakekeeper 컨테이너)

이러면 `.env` source 가 default 를 깨지 않음. 단, plan 본문은 단일 `MINIO_ENDPOINT` 가정이고 본 fix 의 default 만 바꿔도 정상 동작 시점에 도달했으므로 Phase 1A 범위에서는 후속 과제로 미룸.

### 교훈
`localhost` 는 컨테이너 안에서 의미가 다름. docker-compose 네트워킹은 service-name DNS 를 default 로 가정하는 게 안전.

---

## Issue 4 — Fresh instance 는 server-level bootstrap 필요

### 증상
Issue 3 fix 후 bootstrap.py 첫 실행에서 `RuntimeError: no project found in Lakekeeper` — `/management/v1/project-list` 응답이 빈 배열.

### 원인
Lakekeeper v0.5.x 는 첫 기동 후 **server-level bootstrap 을 한 번 명시적으로 호출** 해야 default project 를 생성. 그 전에는 project-list 가 비어 있어 후속 모든 management API 호출이 무의미.

### 해결
`bootstrap.py` 에 `ensure_server_bootstrapped()` 함수 추가:

```python
def ensure_server_bootstrapped(client: httpx.Client) -> None:
    r = client.post(
        f"{LAKEKEEPER_URL}/management/v1/bootstrap",
        json={"accept-terms-of-use": True},
    )
    if r.status_code in (400, 409):
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        error_block = body.get("error")
        error_type = error_block.get("type", "") if isinstance(error_block, dict) else ""
        if "already" in error_type.lower() or r.status_code == 409:
            return  # already bootstrapped — idempotent
    if r.status_code >= 400:
        r.raise_for_status()
```

멱등성 처리:
- 첫 호출: 200 OK (server 가 bootstrapped 상태로 진입)
- 재호출: **400** with `{"error": {"type": "CatalogAlreadyBootstrapped", ...}}` (409 가 아님 — 일부 docs 가 잘못 표기)

### 검증
첫 실행: `created warehouse 'seoul'`
재실행: `warehouse 'seoul' already exists, skipping` (멱등)

### 교훈
v0.5.x 의 docs 에는 bootstrap 호출이 명확하지 않음 — OpenAPI 스펙 (`http://localhost:8181/api/openapi.json`) 의 `/management/v1/bootstrap` path 가 진실 source. 외부 docs 보다 라이브 스키마.

---

## Bonus — Error response 형식 가드 (code review 가 발견)

### 발견 경로
Issue 4 fix 후 code-reviewer subagent 가 dispatch 됨. review 결과:

> `body.get("error", {}).get("type", "")` — `error` 가 dict 가 아닌 경우 AttributeError. Cloudflare Tunnel 등 일반적인 HTTP 프록시가 plain-text 400 을 반환할 때 발생 가능.

### 해결
`isinstance` 가드 한 줄 추가 (`7b79c3d`):

```python
error_block = body.get("error")
error_type = error_block.get("type", "") if isinstance(error_block, dict) else ""
```

### 의의
self-review 가 아닌 **fresh subagent code review 가 실 운영 edge case 를 잡은 사례**. Cloudflare Tunnel 은 Phase 1B 에서 도입 예정이라 본 가드가 향후 발동 가능성 있음.

---

## 적용된 fallback 정책

plan 본문 §9-1 의 Lakekeeper 디버깅 2시간 초과 시 JdbcCatalog 우회 트리거는 **발동하지 않음**. 위 4가지 이슈 진단 + fix 합쳐 ~35분 (모두 `--help` / OpenAPI / docker logs / docker inspect 만 사용). 정상 경로로 closure.

## 향후 운영 시 주의

| 상황 | 조치 |
|---|---|
| Lakekeeper 컨테이너가 즉시 exit (1) | logs 의 `relation "X" does not exist` 패턴 확인 → migrate init 실행 여부 점검 |
| `(health: starting)` 영원히 | 컨테이너 안 binary 자체가 healthcheck subcommand 제공하는지 확인 (`--help`) |
| storage validation 실패 | endpoint 가 docker-internal 호스트네임인지 확인 |
| project-list 빈 배열 | server-level bootstrap 호출 (`accept-terms-of-use=true`) |
| HTTP 프록시 뒤에 둘 때 | error response 가 JSON 이 아닐 가능성 — 가드 필수 |

`docs/runbook/day1_infra.md` 의 troubleshooting 표에 짧게 인덱스로 추가 예정.
