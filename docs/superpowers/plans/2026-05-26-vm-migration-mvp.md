# VM 마이그레이션 MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mac 로컬에 구축된 데이터 플랫폼의 "공개 데모 임계경로"를 Oracle Cloud VM 으로 재현해, `seoulnow.live` 지도가 노트북이 꺼져도 VM 에서 서빙되는 실데이터를 표시하게 한다.

**Architecture:** 인프라(kafka/postgres/minio/lakekeeper)는 VM 의 docker-compose 로, 스트리밍/serving(hotspot_producer → bronze_to_silver → silver_to_gold → FastAPI)은 systemd 호스트 프로세스로 가동한다. 데이터는 재적재(re-ingest), 노출은 기존 cloudflared tunnel 에 `api.seoulnow.live → :8000` ingress 추가, 이후 업데이트는 GitHub Actions ssh 자동 배포.

**Tech Stack:** Oracle Cloud Ubuntu 24.04 aarch64 (4 OCPU / 24GB / 50GB), Docker Compose, systemd, uv, PyFlink 1.20, Iceberg + Lakekeeper, DuckDB, FastAPI, cloudflared, GitHub Actions, Cloudflare Pages.

**SoT:** spec `docs/superpowers/specs/2026-05-25-vm-migration-design.md`. VM 전용 API 키 값 = `.local-notes/vm-secrets.md`(미커밋). 호스트 setup 절차 = `README.md` L84-91.

---

## 실행 주체 표기

- **[REPO]** = 리포지토리 파일 생성/수정. subagent/inline 으로 구현 + PR 가능 (Task 1-4).
- **[VM]** = VM 에 ssh 로 사용자가 직접 실행하는 인터랙티브 ops (Task 5-8). plan 은 정확한 명령 + 기대 출력을 제공하고, 사용자가 실행한다.

---

## File Structure (신규/수정)

```
seoul-citydata-platform/
├── infra/systemd/                              [REPO 신규]
│   ├── seoulnow-hotspot-producer.service
│   ├── seoulnow-bronze-silver.service
│   ├── seoulnow-silver-gold.service
│   ├── seoulnow-api.service
│   └── install-units.sh
├── infra/cloudflare/tunnel-config.example.yml  [REPO 수정 — api ingress 추가]
├── infra/vm/deploy.sh                           [REPO 신규]
├── .github/workflows/deploy-vm.yml              [REPO 신규]
└── docs/runbook/vm-migration-mvp.md             [REPO 신규 — VM ops 절차 SoT]
```

---

## Conventions

- commit 한글 / type-scope 영어 / `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>` trailer.
- branch `vm-migration/mvp` (또는 Task 단위 분할 시 `vm-migration/<short-desc>`).
- 인프라 작업은 TDD 대신 **검증 단계 명시**(명령 + 기대 출력).
- systemd 유닛 경로 가정: VM user=`ubuntu`, repo=`/home/ubuntu/seoulnow`, uv=`/home/ubuntu/.local/bin/uv`. 다르면 Task 5 에서 `which uv`로 확인 후 유닛 수정.
- **Airflow / kafka-connect / subway / CDC / receiver 는 본 MVP 제외** (레이어링 2차). compose 기동 시 명시적 서비스 목록으로 제외한다.

---

## Task 1: systemd 유닛 4종 + 설치 스크립트 [REPO]

**Files:**
- Create: `infra/systemd/seoulnow-hotspot-producer.service`
- Create: `infra/systemd/seoulnow-bronze-silver.service`
- Create: `infra/systemd/seoulnow-silver-gold.service`
- Create: `infra/systemd/seoulnow-api.service`
- Create: `infra/systemd/install-units.sh`

- [ ] **Step 1: hotspot-producer 유닛 작성**

`infra/systemd/seoulnow-hotspot-producer.service`:
```ini
[Unit]
Description=seoulnow hotspot producer (citydata 5min polling)
After=network-online.target docker.service
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/seoulnow
EnvironmentFile=/home/ubuntu/seoulnow/.env
ExecStart=/home/ubuntu/.local/bin/uv run python -m producers.hotspot_producer
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: bronze-silver 유닛 작성**

`infra/systemd/seoulnow-bronze-silver.service`:
```ini
[Unit]
Description=seoulnow flink bronze_to_silver
After=network-online.target docker.service
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/seoulnow
EnvironmentFile=/home/ubuntu/seoulnow/.env
Environment=MINIO_ENDPOINT=http://localhost:9000
Environment=LAKEKEEPER_URL=http://localhost:8181
Environment=PYTHONPATH=src
ExecStart=/home/ubuntu/.local/bin/uv run --extra flink python -m flink_jobs.bronze_to_silver
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 3: silver-gold 유닛 작성**

`infra/systemd/seoulnow-silver-gold.service`:
```ini
[Unit]
Description=seoulnow flink silver_to_gold
After=network-online.target docker.service
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/seoulnow
EnvironmentFile=/home/ubuntu/seoulnow/.env
Environment=MINIO_ENDPOINT=http://localhost:9000
Environment=LAKEKEEPER_URL=http://localhost:8181
Environment=PYTHONPATH=src
ExecStart=/home/ubuntu/.local/bin/uv run --extra flink python -m flink_jobs.silver_to_gold
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 4: api 유닛 작성**

`infra/systemd/seoulnow-api.service`:
```ini
[Unit]
Description=seoulnow serving FastAPI (chill-open / hotspots)
After=network-online.target docker.service
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/seoulnow
EnvironmentFile=/home/ubuntu/seoulnow/.env
Environment=MINIO_ENDPOINT=http://localhost:9000
Environment=LAKEKEEPER_URL=http://localhost:8181
Environment=PYTHONPATH=src
ExecStart=/home/ubuntu/.local/bin/uv run uvicorn api.main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 5: 설치 스크립트 작성**

`infra/systemd/install-units.sh`:
```bash
#!/usr/bin/env bash
# infra/systemd/*.service 를 /etc/systemd/system 에 설치 + enable.
# VM 에서 실행: bash infra/systemd/install-units.sh
set -euo pipefail

SRC="$(cd "$(dirname "$0")" && pwd)"
UNITS=(seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold seoulnow-api)

for u in "${UNITS[@]}"; do
  sudo cp "$SRC/$u.service" "/etc/systemd/system/$u.service"
done

sudo systemctl daemon-reload
sudo systemctl enable "${UNITS[@]}"
echo "installed + enabled: ${UNITS[*]}"
echo "start: sudo systemctl start ${UNITS[*]}"
```

- [ ] **Step 6: 셸 문법 검증**

Run: `bash -n infra/systemd/install-units.sh && echo OK`
Expected: `OK`

- [ ] **Step 7: 유닛 구조 검증 (로컬 best-effort)**

Run: `grep -L "ExecStart=" infra/systemd/*.service`
Expected: 출력 없음 (4개 유닛 모두 ExecStart 포함). 정식 `systemd-analyze verify` 는 VM(Task 6)에서 수행.

- [ ] **Step 8: Commit**

```bash
git add infra/systemd/
git commit -m "feat(infra): VM systemd 유닛 4종 + 설치 스크립트

producers/flink/serving 호스트 프로세스를 VM 부팅 영속화. EnvironmentFile
=.env + MINIO/LAKEKEEPER localhost override (run_api.sh 패턴 정합).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: tunnel-config 에 serving ingress 추가 [REPO]

**Files:**
- Modify: `infra/cloudflare/tunnel-config.example.yml`

- [ ] **Step 1: api ingress 추가**

`infra/cloudflare/tunnel-config.example.yml` 전체를 아래로 교체:
```yaml
# Cloudflare Tunnel config 템플릿.
# 복사: cp infra/cloudflare/tunnel-config.example.yml ~/.cloudflared/config.yml
# UUID / 도메인은 본인 계정 값으로 교체. credentials 본문은 *절대* commit 금지.
#
# 참고: 본 파일은 .example.yml 만 commit 대상. 실 값이 박힌 tunnel-config.yml 은
# .gitignore 로 차단되어 있다 (Cloudflare 영역 참조).

tunnel: <YOUR_TUNNEL_UUID>
credentials-file: /home/ubuntu/.cloudflared/<YOUR_TUNNEL_UUID>.json

ingress:
  # serving FastAPI (chill-open / hotspots) — VM 마이그레이션 MVP
  - hostname: api.seoulnow.live
    service: http://localhost:8000
  # Edge events HTTP receiver (Day 11 Task 11.2) — docker profile=receiver
  - hostname: receiver.seoulnow.live
    service: http://localhost:8400
  - service: http_status:404
```

- [ ] **Step 2: YAML 유효성 검증**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('infra/cloudflare/tunnel-config.example.yml')); print('YAML OK')"`
Expected: `YAML OK`

- [ ] **Step 3: catchall 존재 확인**

Run: `grep -c "http_status:404" infra/cloudflare/tunnel-config.example.yml`
Expected: `1` (마지막 catchall 필수 — 없으면 cloudflared 안 뜸)

- [ ] **Step 4: Commit**

```bash
git add infra/cloudflare/tunnel-config.example.yml
git commit -m "feat(infra): tunnel config 에 api.seoulnow.live serving ingress 추가

serving FastAPI(:8000) 를 기존 receiver tunnel 에 hostname 추가로 노출.
VM 마이그레이션 MVP 의 CHILL_API_BASE 대상.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: deploy.sh + GitHub Actions 자동 배포 [REPO]

**Files:**
- Create: `infra/vm/deploy.sh`
- Create: `.github/workflows/deploy-vm.yml`

- [ ] **Step 1: deploy.sh 작성**

`infra/vm/deploy.sh`:
```bash
#!/usr/bin/env bash
# VM 갱신 배포 — git pull + 인프라 compose(MVP 서비스만) + 호스트 프로세스 restart.
# GitHub Actions(ssh) + 수동 양쪽에서 사용. 첫 부트스트랩은 runbook 참조.
set -euo pipefail

cd /home/ubuntu/seoulnow
git pull --ff-only

# 인프라 compose — MVP 서비스만 (airflow / kafka-connect 제외).
docker compose up -d kafka postgres minio minio-bootstrap lakekeeper-migrate lakekeeper

# 호스트 프로세스 재시작.
sudo systemctl restart \
  seoulnow-hotspot-producer \
  seoulnow-bronze-silver \
  seoulnow-silver-gold \
  seoulnow-api

echo "deploy done."
```

- [ ] **Step 2: GitHub Actions 워크플로우 작성**

`.github/workflows/deploy-vm.yml`:
```yaml
name: deploy-vm

on:
  push:
    branches: [main]
    paths-ignore:
      - 'frontend/**'
      - 'docs/**'
      - '**/*.md'

# 동시 배포 충돌 방지.
concurrency:
  group: deploy-vm
  cancel-in-progress: false

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: ssh deploy
        uses: appleboy/ssh-action@v1.2.0
        with:
          host: ${{ secrets.VM_HOST }}
          username: ${{ secrets.VM_USER }}
          key: ${{ secrets.VM_SSH_KEY }}
          script: bash /home/ubuntu/seoulnow/infra/vm/deploy.sh
```

- [ ] **Step 3: 셸 문법 검증**

Run: `bash -n infra/vm/deploy.sh && echo OK`
Expected: `OK`

- [ ] **Step 4: 워크플로우 YAML 검증**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/deploy-vm.yml')); print('YAML OK')"`
Expected: `YAML OK`

- [ ] **Step 5: Commit**

```bash
git add infra/vm/deploy.sh .github/workflows/deploy-vm.yml
git commit -m "feat(infra): VM 갱신 배포 스크립트 + GitHub Actions ssh 자동 배포

main push(코드 변경) 시 VM ssh → git pull + 인프라 compose + 호스트
프로세스 restart. frontend/docs/md 변경은 paths-ignore 로 제외.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: VM 마이그레이션 MVP runbook [REPO]

**Files:**
- Create: `docs/runbook/vm-migration-mvp.md`

- [ ] **Step 1: runbook 작성**

`docs/runbook/vm-migration-mvp.md` 에 Task 5-8 의 모든 [VM] 명령 + 기대 출력 + 검증 + fallback 을 운영 절차로 정리한다. 본문은 아래 구조를 따른다 (Task 5-8 의 명령을 그대로 복사):
```markdown
# Runbook — VM 마이그레이션 MVP

> SoT spec = docs/superpowers/specs/2026-05-25-vm-migration-design.md
> VM = 155.248.164.17 (ubuntu), repo = ~/seoulnow. VM 전용 API 키 = .local-notes/vm-secrets.md.

## 1. 부트스트랩 (Task 5)
## 2. 데이터 재적재 + systemd (Task 6)
## 3. serving + tunnel + Pages env (Task 7)
## 4. 배포 자동화 (Task 8)
## 5. 운영 명령 / 트러블슈팅
- 메모리: `free -h` (80% 임계 = 19.2GB)
- 로그: `journalctl -u seoulnow-silver-gold -f`
- compose 상태: `docker compose ps`
- 환경 구분: 본 절차는 **VM(prod)** — Mac(dev) 과 물리 분리. 혼동 주의.
```

- [ ] **Step 2: Task 5-8 명령 일치 검증**

Run: `grep -c "seoulnow-" docs/runbook/vm-migration-mvp.md`
Expected: `>= 4` (systemd 유닛 4종 언급)

- [ ] **Step 3: Commit**

```bash
git add docs/runbook/vm-migration-mvp.md
git commit -m "docs(runbook): VM 마이그레이션 MVP 운영 절차

부트스트랩 → 재적재 → serving/tunnel → 배포 자동화 + 트러블슈팅.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: VM 부트스트랩 [VM]

> 실행 주체 = 사용자(VM ssh). `ssh ubuntu@155.248.164.17`.

- [ ] **Step 1: Docker + compose plugin 설치**

```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu && newgrp docker
docker --version && docker compose version
```
Expected: docker / compose 버전 출력 (compose v2 plugin).

- [ ] **Step 2: uv 설치 + repo 최신화**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
which uv   # → /home/ubuntu/.local/bin/uv (다르면 systemd 유닛 ExecStart 수정)
cd ~/seoulnow && git pull --ff-only
uv sync --extra dev --extra flink
```
Expected: `uv` 경로 출력 + `uv sync` 의존성 설치 완료.

- [ ] **Step 3: .env 전체 sync (VM 전용 키로 치환)**

로컬(Mac)에서 VM 으로 `.env` 를 보내되, **API 키 2개는 `.local-notes/vm-secrets.md` 의 VM 전용 값으로 치환**한다. 나머지(minio/postgres creds, RECEIVER_TOKEN, ANON_UA_SALT, KAFKA_BOOTSTRAP_SERVERS 등)는 동일.

VM 에서 확인:
```bash
grep -E "SEOUL_OPENAPI_KEY|SEOUL_SUBWAY_API_KEY|KAFKA_BOOTSTRAP_SERVERS|MINIO_ROOT_USER" ~/seoulnow/.env | sed 's/=.*/=SET/'
```
Expected: 4개 키 모두 `=SET`. (SEOUL_OPENAPI_KEY 는 VM 전용 값 = `476e...`)

- [ ] **Step 4: /etc/hosts 호스트 setup (필수)**

호스트 프로세스가 Lakekeeper REST `overrides.uri=lakekeeper:8181` + S3 endpoint 를 해석하도록 (compose L184-187 / README Quick Start):
```bash
grep -q "127.0.0.1 lakekeeper minio" /etc/hosts || echo "127.0.0.1 lakekeeper minio" | sudo tee -a /etc/hosts
grep "lakekeeper minio" /etc/hosts
```
Expected: `127.0.0.1 lakekeeper minio`

- [ ] **Step 5: ARM 멀티아치 이미지 pull 검증**

```bash
cd ~/seoulnow
for img in apache/kafka:4.0.0 postgres:16 minio/minio:RELEASE.2025-01-20T14-49-07Z minio/mc:RELEASE.2025-01-17T23-25-50Z quay.io/lakekeeper/catalog:v0.12.1; do
  docker pull "$img" >/dev/null && echo "OK  $img" || echo "FAIL $img"
done
```
Expected: 5개 모두 `OK`. **FAIL 시** (특히 lakekeeper) → spec §8 ARM 리스크 발동: 대체 태그/이미지 조사 후 plan 정정.

- [ ] **Step 6: 인프라 compose 기동 (MVP 서비스만)**

```bash
docker compose up -d kafka postgres minio minio-bootstrap lakekeeper-migrate lakekeeper
sleep 30 && docker compose ps
```
Expected: `kafka` / `postgres` / `minio` / `lakekeeper` 가 `healthy`. (`airflow-*` / `kafka-connect` 는 목록에 없어야 정상 — MVP 제외)

- [ ] **Step 7: Kafka 토픽 생성**

```bash
bash infra/kafka/create_topics.sh
docker exec scp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
Expected: `seoul.hotspot.congestion.v1` 등 토픽 목록 출력.

- [ ] **Step 8: 메모리 측정**

Run: `free -h`
Expected: used 가 19.2GB(80%) 하회. 초과 시 spec §8 메모리 리스크 검토.

---

## Task 6: 데이터 재적재 + systemd 가동 [VM]

- [ ] **Step 1: 공공 인허가 정적 적재**

```bash
cd ~/seoulnow && uv run python scripts/load_static_places.py
```
Expected: bronze parquet 적재 완료 로그.

- [ ] **Step 2: systemd 유닛 설치 + 기동**

```bash
bash infra/systemd/install-units.sh
sudo systemctl start seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold
```
Expected: install-units 가 4종 enable. (api 는 Task 7 에서 기동)

- [ ] **Step 3: 유닛 active 검증**

```bash
systemctl is-active seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold
systemd-analyze verify /etc/systemd/system/seoulnow-silver-gold.service || true
```
Expected: 3개 모두 `active`. (systemd-analyze 경고 없으면 양호)

- [ ] **Step 4: 1 cycle(5분) 대기 후 gold 적재 확인**

```bash
sleep 330
journalctl -u seoulnow-silver-gold -n 20 --no-pager
uv run --extra flink python scripts/duckdb_check.py   # gold fact_hotspot_congestion_5min row 확인
```
Expected: gold `fact_hotspot_congestion_5min` 에 row 존재.

- [ ] **Step 5: dbt mart 생성 + 테스트**

```bash
cd ~/seoulnow/dbt/seoul && uv run dbt build
```
Expected: `chill_open_now` 등 mart 생성 + dbt test PASS. (실패 시 `dbt run` → `dbt test` 분리 실행으로 원인 격리)

- [ ] **Step 6: 부팅 영속 확인**

Run: `systemctl is-enabled seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold`
Expected: 3개 모두 `enabled`.

---

## Task 7: serving + tunnel + Pages env [VM + Cloudflare]

- [ ] **Step 1: serving FastAPI 유닛 기동 + 로컬 확인**

```bash
sudo systemctl start seoulnow-api
sleep 5
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8000/api/hotspots
```
Expected: `200` (실데이터). 503/500 시 `journalctl -u seoulnow-api -n 50`.

- [ ] **Step 2: tunnel 에 api ingress 추가**

```bash
# config.yml 에 api.seoulnow.live → :8000 ingress 추가 (receiver 블록 위, catchall 아래).
sudo nano /etc/cloudflared/config.yml   # tunnel-config.example.yml 형태로 편집
cloudflared tunnel route dns seoulnow-receiver api.seoulnow.live
sudo systemctl restart cloudflared
```
Expected: `Added CNAME api.seoulnow.live ...` + cloudflared active.

- [ ] **Step 3: 외부 HTTPS 검증**

(로컬 macOS 에서) Run: `curl -s -o /dev/null -w "%{http_code}\n" https://api.seoulnow.live/api/hotspots`
Expected: `200` (Cloudflare edge → tunnel → VM:8000 → 실데이터).

- [ ] **Step 4: Cloudflare Pages env 설정**

Cloudflare dashboard → Pages(seoulnow) → Settings → Environment variables (Production):
```
CHILL_API_BASE=https://api.seoulnow.live
EVENTS_RECEIVER_BASE=https://receiver.seoulnow.live
RECEIVER_TOKEN=<.env 값>
ANON_UA_SALT=<.env 값>
```
또는 CLI: `wrangler pages secret put CHILL_API_BASE --project-name=seoulnow` (값 입력).
Expected: 4개 변수 등록.

- [ ] **Step 5: Pages 재배포 + 지도 실데이터 검증**

main 에 빈 commit push 또는 dashboard 에서 "Retry deployment" → 빌드 완료 후 `https://seoulnow.live` 접속.
Expected: 지도가 **혼잡도 색상 + chill-open 마커** 표시 + degraded 배너 소멸.

- [ ] **Step 6: prod 독립성 검증 (핵심 게이트)**

Mac 의 docker/스트리밍/run_api 를 모두 정지(또는 노트북 종료) → `https://seoulnow.live` 재접속.
Expected: 지도가 **여전히 실데이터 표시** (VM 이 독립 서빙). ← MVP 성공의 결정적 증거.

---

## Task 8: 배포 자동화 활성화 [GitHub + VM]

- [ ] **Step 1: sudoers NOPASSWD (systemctl restart)**

```bash
echo "ubuntu ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold seoulnow-api" | sudo tee /etc/sudoers.d/seoulnow-deploy
sudo chmod 440 /etc/sudoers.d/seoulnow-deploy && sudo visudo -c
```
Expected: `parsed OK`. (GitHub Actions 가 비대화식 sudo restart 가능하게)

- [ ] **Step 2: GitHub repo secrets 등록**

GitHub repo → Settings → Secrets and variables → Actions:
```
VM_HOST   = 155.248.164.17
VM_USER   = ubuntu
VM_SSH_KEY = <VM ssh private key 전체>
```
Expected: 3개 secret 등록.

- [ ] **Step 3: deploy.sh 실행 권한 + 워크플로우 main 반영**

VM: `chmod +x ~/seoulnow/infra/vm/deploy.sh` (PR 머지 후 git pull 로 들어옴).
워크플로우(`deploy-vm.yml`)가 main 에 있어야 트리거됨 → 본 plan 의 PR 머지 시점에 활성화.

- [ ] **Step 4: 테스트 push 반영 검증**

코드 경로(예: `src/producers/`)에 무해한 변경 1줄 → commit → push main → Actions `deploy-vm` 성공 → VM 에서 `git log -1` 가 해당 commit.
Expected: Actions 녹색 + VM systemd 유닛 restart 로그 + `git log -1` 일치.

---

## 검증 체크리스트 (MVP 완료 게이트 = spec §10)

- [ ] VM 인프라 compose 4서비스 healthy
- [ ] VM gold `fact_hotspot_congestion_5min` row 적재 + dbt mart PASS
- [ ] `seoulnow-*` 유닛 active + enabled
- [ ] `curl https://api.seoulnow.live/api/hotspots` → 200 + 실데이터
- [ ] `https://seoulnow.live` 지도 실데이터 + degraded 소멸
- [ ] **Mac 정지 후에도 지도 유지** (prod 독립성)
- [ ] `deploy-vm` 워크플로우 테스트 push 반영
