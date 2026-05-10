# Seoul Citydata Platform

서울 공공 실시간 데이터(도시데이터·지하철 혼잡도) 와 Postgres CDC, 익명 사용자 행동 로그를 Kafka 메시지 버스로 통합하고 PyFlink streaming + Spark batch + Iceberg(Lakekeeper) + dbt + GitHub Actions 로 처리하는 1인 운영 데이터 플랫폼.

## Quick Start (로컬 docker-compose)

```bash
# 0. host /etc/hosts 에 docker hostname alias 1회 추가 (sudo 1회).
#    Lakekeeper REST 가 `/v1/config` 의 `overrides.uri` 로 vend 하는 catalog uri
#    가 docker hostname 기준이라 host 측 client (PyFlink, dbt host run 등) 도 같은
#    hostname 으로 resolve 되어야 한다. host port mapping (8181:8181, 9000:9000)
#    덕분에 alias 1줄로 host / container 양쪽이 통과한다 (자세한 진단은
#    docs/runbook/day1_infra.md 의 트러블슈팅 표).
sudo sh -c 'echo "127.0.0.1 lakekeeper minio" >> /etc/hosts'

# 1. .env 복사 — `LAKEKEEPER_URL=http://lakekeeper:8181`,
#    `MINIO_ENDPOINT=http://minio:9000` 이 default.
cp .env.example .env
# .env 의 SEOUL_OPENAPI_KEY, SEOUL_SUBWAY_API_KEY 채우기

docker compose up -d
./scripts/healthcheck.sh

# 토픽 생성
./infra/kafka/create_topics.sh

# Lakekeeper warehouse 등록
uv run python infra/lakekeeper/bootstrap.py
```

## 문서

- 프로젝트 컨텍스트: [`CLAUDE.md`](./CLAUDE.md)
- Phase 1 spec: [`docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md`](./docs/superpowers/specs/2026-04-30-seoul-citydata-platform-phase1-design.md)
- Phase 1A Week 1 plan: [`docs/superpowers/plans/phase-1a-week-1.md`](./docs/superpowers/plans/phase-1a-week-1.md)

## 비용

운영 비용 월 $0~$2 (Oracle Cloud Always Free + Cloudflare 무료 + 공공 무료 API).
