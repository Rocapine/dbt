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
  select distinct source.rc_original_app_user_id
  from source   
  join cohort using (rc_original_app_user_id)
  where source.is_trial_period = true
    and source.is_sandbox = false
    and coalesce(source.ownership_type, 'PURCHASED') <> 'FAMILY_SHARED'
),
converted as (
  select distinct source.rc_original_app_user_id
  from source
  join trials as tr using (rc_original_app_user_id)
  group by source.rc_original_app_user_id
  having countif(source.is_trial_conversion) > 0
  and countif(source.refunded_at is not null) = 0
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

select
  b.rc_original_app_user_id as revenuecat_id,
  min(date(b.first_seen_time)) as first_seen_date,
  any_value(b.country) as country,
  any_value(b.platform) as platform,
  max(b.refunded_at) as has_refunded,
  max(case when tr.rc_original_app_user_id is not null then true else false end) as has_started_trial,
  max(case when converted.rc_original_app_user_id is not null then true else false end) as has_converted_trial,
  any_value(b.product_identifier) as product_id,
  max(b.purchase_price_in_usd) as purchased_price_usd,
  case
    when lower(any_value(b.country)) in ('al','at','by','be','ba','bg','hr','cz','dk','de','ee','fi','gr','hu','is','ie','it','lt','lv','lu','mk','mt','md','nl','no','pl','pt','ro','ru','rs','sk','si','es','se','ch','tr','ua','cy') then 'EUROPE'
    when lower(any_value(b.country)) in ('us','ca') then 'US/CA'
    else 'Rest of The world'
  end as region
from base b
left join trials tr on tr.rc_original_app_user_id = b.rc_original_app_user_id
left join converted on converted.rc_original_app_user_id = b.rc_original_app_user_id
group by b.rc_original_app_user_id
