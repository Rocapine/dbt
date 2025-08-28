{{ config(materialized='table') }}

with harmony as (
  select
    'harmony' as app_name,
    date(first_seen_time) as first_seen_date,
    count(distinct if(is_trial_period, rc_original_app_user_id, null)) as trial_starts,
    count(distinct if(is_trial_conversion and unsubscribe_detected_at is null, rc_original_app_user_id, null)) as converted_users
  from {{ source('raw', 'RevenueCatHarmonyTer') }}
  group by 1, 2
),
unchained as (
  select
    'unchained' as app_name,
    date(first_seen_time) as first_seen_date,
    count(distinct if(is_trial_period, rc_original_app_user_id, null)) as trial_starts,
    count(distinct if(is_trial_conversion and unsubscribe_detected_at is null, rc_original_app_user_id, null)) as converted_users
  from {{ source('raw', 'RevenueCatUnchaind') }}
  group by 1, 2
)
select * from harmony
union all
select * from unchained
order by first_seen_date, app_name


