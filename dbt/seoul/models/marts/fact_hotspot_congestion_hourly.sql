-- Gold mart: 자치구 × 시각 시간단위 평균 (PyFlink 의 5분 윈도우와 별개로 batch 집계).
--
-- plan Task 5.2 deviation — staging 의 `congest_level_score > 0` filter 를 본
-- mart 로 이동. 이유: Task 5.3 의 singular test 가 staging ref 로 score = 0
-- row 까지 검증해야 함 (region 매핑 안 된 row 의 congest_level 라벨 4종 외
-- 발견 검증). staging 은 unfiltered raw projection.
{{ config(
    materialized='table',
    schema='gold'
) }}

select
    date_trunc('hour', api_response_ts) as window_hour,
    district,
    any_value(gu_code) as gu_code,
    count(distinct area_code) as area_count,
    avg(congest_level_score) as avg_congest_score,
    max(congest_level_score) as max_congest_score,
    avg(population_min) as avg_population_min,
    avg(population_max) as avg_population_max,
    max(silver_arrival_ts) as last_silver_arrival_ts
from {{ ref('stg_hotspot_silver') }}
where congest_level_score > 0
  and api_response_ts >= now() - interval 7 day
group by 1, 2
