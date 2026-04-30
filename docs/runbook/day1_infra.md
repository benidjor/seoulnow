# Day 1 Infra Runbook

## 평소 기동
```bash
docker compose up -d
./scripts/healthcheck.sh
```

## 정지
```bash
docker compose down            # 컨테이너만 정지, volume 유지
docker compose down -v         # volume 까지 삭제 (데이터 전부 날아감)
```

## 로그 확인
```bash
docker compose logs -f kafka
docker compose logs -f lakekeeper
```

## 메모리 모니터
```bash
./scripts/memory_watch.sh                # 80% 기본
THRESHOLD=70 ./scripts/memory_watch.sh   # 70% 로 더 빡빡하게
```

## 자주 발생하는 문제

| 증상 | 원인 / 조치 |
|---|---|
| `lakekeeper` healthcheck 가 계속 실패 | Postgres 마이그레이션 지연. 60초 더 대기. 그래도 실패하면 `docker compose logs lakekeeper` 확인 후 spec §9-1 fallback (JdbcCatalog) 발동. |
| `kafka-topics.sh` 가 connection refused | Kafka KRaft 컨트롤러가 아직 안 떴음. 30초 대기 후 재시도. |
| MinIO 콘솔 접속 안 됨 | 9001 포트 충돌. `lsof -i :9001` 로 확인. |
| 메모리 80% 초과 | Spark 가 떠 있는지 확인 (Day 9 외에는 안 떠 있어야 함). 또는 Flink TaskManager heap 축소. |
