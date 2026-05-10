-- Gold mart: 현재 활성 가게 1행 / place_id (SCD2 폐쇄 전 단계 view).
--
-- silver.dim_place 는 SCD2 골격 (각 CDC 변경마다 append) 이라 한 가게에 여러 행이
-- 쌓인다. 본 view 는 `valid_from desc` 기준 latest 1행만 추출 + delete 행 제거 +
-- status='active' 만. Day 9 Spark MERGE 가 valid_to 를 닫고 is_current=false 로
-- 갱신하면, 본 view 는 단순히 `is_current = true` 만 필터하도록 정리할 수 있다.
{{ config(materialized='view', schema='gold') }}

with ranked as (
    select
        *,
        row_number() over (partition by place_id order by valid_from desc) as rn
    from {{ source('silver', 'dim_place') }}
)
select
    place_id, biz_reg_no, name, category, district, gu_code,
    latitude, longitude, open_hour, close_hour, status,
    valid_from
from ranked
where rn = 1
  and cdc_op <> 'd'
  and status = 'active'
