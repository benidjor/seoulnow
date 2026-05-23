# frontend/public 정적 자산

## seoul-districts.geojson

**현재 상태**: 5 자치구 (강남/마포/종로/성동/용산) rectangle placeholder. Task 11.0 첫 push 의 Cloudflare Pages 자동 배포 검증 + 지도 렌더 확인 용도.

**production 교체 권장** (Day 12 또는 별도 hygiene commit):

서울 25 자치구 정식 폴리곤 GeoJSON 은 다음 공개 출처에서 무료로 받을 수 있습니다:

- `https://raw.githubusercontent.com/southkorea/seoul-maps/master/kostat/2013/json/seoul_municipalities_geo_simple.json` (단순화, ~150KB)
- `https://raw.githubusercontent.com/southkorea/seoul-maps/master/kostat/2013/json/seoul_municipalities_geo.json` (정밀, ~3MB)

교체 절차:

```bash
cd frontend/public
curl -L -o seoul-districts.geojson \
  https://raw.githubusercontent.com/southkorea/seoul-maps/master/kostat/2013/json/seoul_municipalities_geo_simple.json
```

이후 `CongestionMap.tsx` 는 코드 변경 없이 동작합니다 (`SIG_KOR_NM` properties key 동일).
