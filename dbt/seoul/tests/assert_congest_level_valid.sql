-- 한국어 congest_level 라벨이 매핑 4종 ('여유' / '보통' / '약간 붐빔' / '붐빔') 안에
-- 있는지 검증. 매핑 밖 라벨이 들어오면 producer/transforms 가 깨진 것.
--
-- plan Task 5.2 deviation 의 후속 — score = 0 row 도 staging 에 포함되어
-- 본 test 가 region 매핑 안 된 row 의 congest_level 까지 검증. plan 의
-- source 호출 (silver.hotspot_congestion) 은 sources.yml 제거 deviation
-- 으로 ref 호출 (stg_hotspot_silver) 로 변경.
-- (Jinja 표기 회피 — dbt parser 가 comment 안 source/ref 도 dependency 로
-- 해석하기 때문.)

select
    distinct congest_level
from {{ ref('stg_hotspot_silver') }}
where congest_level not in ('여유', '보통', '약간 붐빔', '붐빔')
  and congest_level is not null
