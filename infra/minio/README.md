# MinIO buckets

`docker compose up` 시 `minio-bootstrap` 컨테이너가 자동으로 다음 버킷을 만든다:

- `seoul-warehouse` — Iceberg warehouse (모든 Bronze/Silver/Gold 테이블)
- `lakekeeper` — Lakekeeper 자체 메타데이터 보관용 (예약)

콘솔: http://localhost:9001 (minioadmin / minioadmin)

수동 재생성:
```bash
docker compose exec minio mc mb local/seoul-warehouse
```
