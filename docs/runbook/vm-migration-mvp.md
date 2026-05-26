# Runbook — VM 마이그레이션 MVP

> SoT spec = docs/superpowers/specs/2026-05-25-vm-migration-design.md
> plan = docs/superpowers/plans/2026-05-26-vm-migration-mvp.md
> VM = 155.248.164.17 (user=ubuntu), repo = ~/seoulnow. VM 전용 API 키 = .local-notes/vm-secrets.md (미커밋).
> ⚠️ 본 절차는 **VM(prod)** 대상 — Mac(dev) 과 물리 분리된 환경. 혼동 주의.

## 1. 부트스트랩 (plan Task 5)

### 1-1. Docker + compose plugin
```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu && newgrp docker
docker --version && docker compose version
```

### 1-2. 빌드 의존성 + uv + Python 3.11 + 의존성

VM(minimal Ubuntu)엔 컴파일러·JDK 부재 → pemja(PyFlink C 확장) 빌드 + Flink JVM 런타임 위해 `build-essential` + JDK 17 필수. Ubuntu 24.04 기본 Python 3.12 는 distutils 제거로 pemja 빌드 실패 → 3.11 고정(Mac dev 와 일치).
```bash
sudo apt update && sudo apt install -y build-essential openjdk-17-jdk
export JAVA_HOME="$(readlink -f "$(which java)" | sed 's:/bin/java::')"   # → /usr/lib/jvm/java-17-openjdk-arm64
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env                                              # uv PATH 즉시 반영
cd ~/seoulnow && git pull --ff-only
echo "3.11" > .python-version                                            # uv 3.11 타겟 (3.12 distutils 회피, systemd uv run 안정화)
uv python install 3.11
uv sync --extra dev --extra flink                                        # apache-flink + pemja 빌드 (~1-2분)
uv run python --version                                                  # → 3.11.x 확인
bash infra/flink/download_jars.sh                                        # Iceberg/Kafka/Hadoop connector JAR 5개(~138MB) → PyFlink lib 동기화
```
> JAVA_HOME 은 flink systemd 유닛(`seoulnow-bronze-silver` / `seoulnow-silver-gold`)에 `Environment=` 로 박혀 있어 런타임엔 별도 설정 불필요. 위 `export` 는 `uv sync`(pemja 빌드) 용.
>
> ⚠️ `download_jars.sh` 는 `.gitignore` 차단(jars/ 미커밋)이라 신규 VM 에서 **반드시 실행**. 생략 시 flink job 이 `MalformedURLException: no protocol` 로 기동 실패. 스크립트가 `uv sync --extra flink` 로 설치된 PyFlink lib 경로를 자동 탐지해 JAR 를 복사하므로 `uv sync` 이후 실행.

### 1-3. .env sync (VM 전용 키 치환)
- Mac `.env` 를 VM 으로 전송하되 `SEOUL_OPENAPI_KEY` / `SEOUL_SUBWAY_API_KEY` 는 `.local-notes/vm-secrets.md` 의 VM 전용 값으로 치환. 나머지(minio/postgres creds, RECEIVER_TOKEN, ANON_UA_SALT, KAFKA_BOOTSTRAP_SERVERS)는 동일.
- 확인: `grep -E "SEOUL_OPENAPI_KEY|SEOUL_SUBWAY_API_KEY|KAFKA_BOOTSTRAP_SERVERS|MINIO_ROOT_USER" ~/seoulnow/.env | sed 's/=.*/=SET/'` → 4개 SET

### 1-4. /etc/hosts (필수 — Lakekeeper REST override 해석)
```bash
grep -q "127.0.0.1 lakekeeper minio" /etc/hosts || echo "127.0.0.1 lakekeeper minio" | sudo tee -a /etc/hosts
```

### 1-5. ARM 멀티아치 이미지 pull 검증
```bash
for img in apache/kafka:4.0.0 postgres:16 minio/minio:RELEASE.2025-01-20T14-49-07Z minio/mc:RELEASE.2025-01-17T23-25-50Z quay.io/lakekeeper/catalog:v0.12.1; do
  docker pull "$img" >/dev/null && echo "OK  $img" || echo "FAIL $img"
done
```
FAIL 시(특히 lakekeeper) spec §8 ARM 리스크 발동 — 대체 태그/이미지 조사 후 정정.

### 1-6. 인프라 compose (MVP 서비스만)
```bash
docker compose up -d kafka postgres minio minio-bootstrap lakekeeper-migrate lakekeeper
sleep 30 && docker compose ps   # kafka/postgres/minio/lakekeeper healthy, airflow/kafka-connect 없어야 정상
```

### 1-7. Lakekeeper warehouse 등록 (필수 — flink Iceberg sink 전제)

신규 Lakekeeper 는 warehouse 가 비어 있어 flink job 의 Iceberg sink 가 catalog 를 못 찾음 → `bootstrap.py` 로 `seoul` warehouse 를 멱등 등록(이미 있으면 storage-profile 동기화만).
```bash
MINIO_ENDPOINT=http://minio:9000 uv run --with httpx python infra/lakekeeper/bootstrap.py
# → "server bootstrapped (or already was)" + "created warehouse 'seoul'" (또는 exists … syncing)
```
> ⚠️ `MINIO_ENDPOINT=http://minio:9000` (도커 내부 hostname) **필수**. Lakekeeper 컨테이너가 이 endpoint 를 warehouse storage-profile 에 박아 직접 MinIO 에 접근하므로 `localhost:9000` 로 두면 컨테이너가 MinIO 에 못 닿음. 호스트 프로세스(PyFlink / serving)는 §1-4 `/etc/hosts` 의 `minio → 127.0.0.1` 로 같은 `minio:9000` 를 해석.

### 1-8. 토픽 생성 + 메모리 측정
```bash
bash infra/kafka/create_topics.sh
docker exec scp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
free -h   # used 19.2GB(80%) 하회
```

## 2. 데이터 재적재 + systemd 가동 (plan Task 6)
```bash
cd ~/seoulnow
uv run python scripts/load_static_places.py
bash infra/systemd/install-units.sh
sudo systemctl start seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold
systemctl is-active seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold
sleep 330
uv run --extra flink python scripts/duckdb_check.py   # gold fact_hotspot_congestion_5min row 확인
systemctl is-enabled seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold
```
> ⚠️ **dbt build 는 MVP serving 에 불필요** — serving(`src/api/routes/`)이 raw gold `fact_hotspot_congestion_5min` + silver + `bronze/places_static_v1/data.parquet` 를 DuckDB 로 직접 read 함(Day 11 Task 11.0 의 Option 5 pivot). `chill_open_now` mart / `dim_place` / CDC 는 후속 레이어링용이라 MVP gold + static 직접 read 경로에선 거치지 않음. 따라서 위 step 에서 `dbt build` 를 제거함.
>
> 📌 **producer 는 3 핫스팟만 커버** — `producers.hotspot_producer` 의 `DEFAULT_AREAS` 가 `POI001~POI003`(강남 / 영등포 / 마포)만 발행해 지도에 자치구 3곳만 색상. 120 핫스팟 전체 확장은 별도 후속 작업(메모리 phase-1b-progress "다음 세션" §5).

### 2-1. (후속 레이어링 — MVP 후) dbt mart 빌드

`chill_open_now` mart / `dim_place` 를 추가할 때만 실행. MVP serving 에는 불필요(위 note). dbt 산출물은 모두 `.gitignore` 차단이라 신규 VM 에서 아래 2 step 선행 필수.
```bash
cp dbt/seoul/profiles.yml.example dbt/seoul/profiles.yml   # gitignored → example 복사
cd ~/seoulnow/dbt/seoul && uv run dbt deps                  # dbt_packages gitignored → dbt_utils 설치
uv run dbt build                                           # mart + test
```
> ⚠️ flink ⊥ dbt 의존성 충돌(`tool.uv.conflicts`, protobuf). dbt 를 호스트에서 돌릴 땐 `flink stop → uv sync --extra dbt → dbt → uv sync --extra dev --extra flink → flink start` 순. MVP 는 dbt 불필요라 flink-set 그대로 유지. (Airflow 레이어링 시엔 별도 dbt-venv 권장.)

## 3. serving + tunnel + Pages env (plan Task 7)
```bash
sudo systemctl start seoulnow-api
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8000/api/hotspots   # 200

# prep 단계 config.yml 은 receiver ingress 만 + 파일 전체 2칸 indent → sed '/^ingress:/' 매칭 실패.
# 부분 편집 대신 전체 재작성(UUID / credentials 는 기존 파일에서 추출, infra/cloudflare/tunnel-config.example.yml 형태).
TUN_UUID=$(awk '/tunnel:/ {print $2; exit}' /etc/cloudflared/config.yml)
TUN_CRED=$(awk '/credentials-file:/ {print $2; exit}' /etc/cloudflared/config.yml)
sudo tee /etc/cloudflared/config.yml >/dev/null <<EOF
tunnel: ${TUN_UUID}
credentials-file: ${TUN_CRED}

ingress:
  - hostname: api.seoulnow.live
    service: http://localhost:8000
  - hostname: receiver.seoulnow.live
    service: http://localhost:8400
  - service: http_status:404
EOF
sudo cloudflared tunnel ingress validate   # config 문법 + ingress 규칙 검증
cloudflared tunnel route dns seoulnow-receiver api.seoulnow.live   # CNAME 자동 추가
sudo systemctl restart cloudflared
```
- (로컬 macOS) `curl -s -o /dev/null -w "%{http_code}\n" https://api.seoulnow.live/api/hotspots` → 200
- Cloudflare Pages(seoulnow) Production env: `CHILL_API_BASE=https://api.seoulnow.live` / `EVENTS_RECEIVER_BASE=https://receiver.seoulnow.live` / `RECEIVER_TOKEN` / `ANON_UA_SALT`
- Pages 재배포 → https://seoulnow.live 지도 실데이터 + degraded 배너 소멸
- **prod 독립성 게이트**: Mac 정지/노트북 종료 후에도 지도 유지 확인 ← MVP 성공 결정적 증거

## 4. 배포 자동화 (plan Task 8)
```bash
echo "ubuntu ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold seoulnow-api" | sudo tee /etc/sudoers.d/seoulnow-deploy
sudo chmod 440 /etc/sudoers.d/seoulnow-deploy && sudo visudo -c   # parsed OK
chmod +x ~/seoulnow/infra/vm/deploy.sh
```
- GitHub repo secrets: `VM_HOST=155.248.164.17` / `VM_USER=ubuntu` / `VM_SSH_KEY=<private key 전체>`
- 테스트 push(`src/` 무해 변경) → Actions `deploy-vm` 성공 → VM `git log -1` 일치

## 5. 운영 / 트러블슈팅
- 메모리: `free -h` (80% 임계 = 19.2GB)
- 로그: `journalctl -u seoulnow-silver-gold -f`
- compose 상태: `docker compose ps`
- 유닛 재시작: `sudo systemctl restart seoulnow-<name>`
- ⚠️ 환경 구분: 본 절차는 VM(prod), Mac(dev)과 분리. Mac·VM 동시 폴링 쿼터는 VM 전용 키로 격리됨.
- 부트스트랩 실측 이슈 11건(pemja distutils / JAVA_HOME systemd / cc / warehouse 미등록 / flink JAR / cloudflared indent / dbt 불필요 발견 등) = [`2026-05-26-vm-migration-bringup.md`](../portfolio/troubleshooting/2026-05-26-vm-migration-bringup.md)
