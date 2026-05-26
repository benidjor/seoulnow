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

### 1-2. uv + repo + 의존성
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
which uv   # → /home/ubuntu/.local/bin/uv (다르면 systemd 유닛 ExecStart 수정)
cd ~/seoulnow && git pull --ff-only
uv sync --extra dev --extra flink
```

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

### 1-7. 토픽 생성 + 메모리 측정
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
cd ~/seoulnow/dbt/seoul && uv run dbt build            # chill_open_now mart + test
systemctl is-enabled seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold
```

## 3. serving + tunnel + Pages env (plan Task 7)
```bash
sudo systemctl start seoulnow-api
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8000/api/hotspots   # 200
# /etc/cloudflared/config.yml 을 infra/cloudflare/tunnel-config.example.yml 형태로 편집 (api ingress 추가)
sudo nano /etc/cloudflared/config.yml
cloudflared tunnel route dns seoulnow-receiver api.seoulnow.live
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
