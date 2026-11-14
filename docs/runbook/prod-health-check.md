# Runbook — prod 헬스체크 (재개 · 정기 점검)

> 대상 = prod (Cloudflare Pages + Tunnel + Oracle VM). Mac (dev)과 혼동하지 않는다.
> 배경 = [`2026-10-21-resume-prod-health-audit.md`](../portfolio/troubleshooting/2026-10-21-resume-prod-health-audit.md) — 5개월 동안 장애를 모른 채 지나간 사례.
> 원칙 = **바깥 계층부터 안쪽으로** 점검하고 처음 실패한 계층에서 멈춰 원인을 좁힌다.

## 1. 요청 경로와 점검 순서

```
브라우저
  ↓ ① DNS (seoulnow.live / seoulnow.pages.dev)
Cloudflare Pages (프론트)
  ↓ ② Pages Functions (/api/v1/*)
Cloudflare Tunnel (api / receiver.seoulnow.live)
  ↓ ③ VM 프로세스 (FastAPI :8000 / receiver :8400)
FastAPI 라우트
  ↓ ④ 데이터 조회 (pyiceberg → DuckDB read_parquet)
Iceberg gold (MinIO · Lakekeeper)
```

## 2. 외부 점검 (로컬 macOS, 1분)

```bash
# ① DNS
for h in seoulnow.live api.seoulnow.live receiver.seoulnow.live; do
  printf "%-26s " $h; dig +short $h | tr '\n' ' '; echo
done

# ①~④ HTTP 계층별
for u in https://seoulnow.pages.dev/ \
         https://seoulnow.pages.dev/api/v1/hotspots \
         https://api.seoulnow.live/ \
         https://api.seoulnow.live/api/hotspots \
         https://receiver.seoulnow.live/; do
  printf "%-46s " $u; curl -s -o /dev/null -m 30 -w "%{http_code} %{time_total}s\n" $u
done
```

### 판정표

| 결과 | 의미 | 다음 행동 |
|---|---|---|
| `seoulnow.live` resolve 실패, 나머지 정상 | apex Custom domain 미연결 | §5 |
| pages.dev 200 + `/api/v1/*` 504 + 빈 `items` | Edge degraded. upstream (api) 문제 | api 행 확인 |
| `api.seoulnow.live/` 404가 즉시 옴 | Tunnel + FastAPI 프로세스 정상 (루트 라우트 없음) | 데이터 라우트 확인 |
| `api.seoulnow.live/` 502 / 530 | Tunnel은 살아 있고 origin (FastAPI) 다운 / Tunnel 다운 | §3 systemd, cloudflared |
| `/api/hotspots` 타임아웃 (`000`) | 데이터 조회 정체 (small file · 디스크 · Flink) | §3 |
| `/api/hotspots` 200 | 정상. 응답 `window_start`가 최근 5~10분 이내인지 확인 | — |
| `receiver.seoulnow.live` 502 | receiver 미기동 (Day 11 e2e 전에는 이 상태가 정상) | `docker compose --profile receiver up -d http-receiver` |

## 3. VM 내부 점검 (읽기 전용)

```bash
ssh ubuntu@155.248.164.17
cd ~/seoulnow

# 자원
df -h /                       # 50GB 디스크 포화 여부
free -h                       # 80% 임계 = 19.2GB
docker compose ps             # kafka / postgres / minio / lakekeeper healthy

# 호스트 프로세스
systemctl status seoulnow-hotspot-producer seoulnow-bronze-silver seoulnow-silver-gold seoulnow-api --no-pager
journalctl -u seoulnow-api --since "1 hour ago" --no-pager | tail -50
journalctl -u seoulnow-silver-gold --since "1 hour ago" --no-pager | tail -50

# gold 파일 · snapshot 수 (small file 누적 판정)
uv run python -c "
from flink_jobs.lib.duckdb_iceberg import build_catalog, table_paths
c = build_catalog()
t = 'gold.fact_hotspot_congestion_5min'
print('files', len(table_paths(c, t)))
print('snapshots', len(c.load_table(t).snapshots()))
"
```

- 파일 수가 수천 개를 넘으면 small file 누적으로 판정한다 (Day 11 실측 기준 9,161 files → read 5~29초).
- 조치: Spark compaction (`infra/spark/jobs/compaction_gold.py`) + `expire_snapshots`. Spark 기동 절차는 [`day9_spark.md`](./day9_spark.md)를 따른다.

## 4. 원천 API 응답 구조 확인 (키 노출 금지)

API 키가 URL 경로에 평문으로 들어가므로 URL · 키를 출력하지 않는다.

```bash
python3 -I - <<'EOF'
import json, re, urllib.parse, urllib.request
key = next(re.match(r'\s*SEOUL_OPENAPI_KEY\s*=\s*(.+)', l).group(1).strip().strip('"\'')
           for l in open('.env', encoding='utf-8') if re.match(r'\s*SEOUL_OPENAPI_KEY\s*=', l))
area = "강남역"   # 장소명 또는 공식 AREA_CD (예: POI014)
url = f"http://openapi.seoul.go.kr:8088/{key}/json/citydata/1/5/" + urllib.parse.quote(area)
d = json.load(urllib.request.urlopen(url, timeout=20))
c = d.get("CITYDATA", {})
print(d.get("RESULT"), c.get("AREA_NM"), c.get("AREA_CD"))
for k, v in c.items():
    if isinstance(v, list) and v and isinstance(v[0], dict):
        print(f"{k}: list[{len(v)}] {list(v[0])[:12]}")
    elif isinstance(v, dict):
        print(f"{k}: dict {list(v)[:12]}")
EOF
```

- `AREA_CD`는 응답 값을 기준으로 삼는다. `hotspot_regions.csv`의 코드와 다르면 [트러블슈팅 Issue 3](../portfolio/troubleshooting/2026-10-21-resume-prod-health-audit.md#issue-3--핫스팟-area_code가-공식-area_cd와-불일치)을 참고한다.

## 5. apex Custom domain 연결 (대시보드)

1. Cloudflare dashboard → `Workers & Pages` → `seoulnow` → `Custom domains`
2. `Set up a domain` → `seoulnow.live` (필요하면 `www.seoulnow.live`도 추가)
3. 1~5분 뒤 확인: `dig +short seoulnow.live` 에 IP가 나오는지, `curl -s -o /dev/null -w "%{http_code}\n" https://seoulnow.live/` → 200

## 6. 점검 주기

- 작업을 재개할 때와 배포 직후에는 반드시 실행한다.
- 정기 점검 자동화 (예: GitHub Actions schedule로 §2 실행 → 실패 시 Discord 알림)는 후속 과제다.
