{{ config(materialized='table') }}

with source as (
  select * from {{ source('raw', 'RevenueCatHarmony') }}
),
cohort as (
  select rc_original_app_user_id
  from source
  group by rc_original_app_user_id
),
trials as (
  select distinct t.rc_original_app_user_id
  from source as t
  join cohort using (rc_original_app_user_id)
  where t.is_trial_period = true
    and t.is_sandbox = false
    and coalesce(t.ownership_type, 'PURCHASED') <> 'FAMILY_SHARED'
),
converted as (
  select distinct t.rc_original_app_user_id
  from source as t
  join trials as tr using (rc_original_app_user_id)
  group by t.rc_original_app_user_id
  having countif(t.is_trial_conversion) > 0
  and countif(t.refunded_at is not null) = 0
),
base as (
  select
    rc_original_app_user_id,
    first_seen_time,
    country,
    platform,
    product_identifier,
    purchase_price_in_usd,
    refunded_at
  from source
)
select distinct rc_original_app_user_id,
    first_seen_time,
    country,
    platform,
    product_identifier,
    purchase_price_in_usd,
    refunded_at
join converted using (rc_original_app_user_id)