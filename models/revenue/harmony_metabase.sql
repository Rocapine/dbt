{{ config(materialized='table') }}

select rc_original_app_user_id as RevenueCatId,
date(first_seen_time) as FirstSeenDate,
country as Country,
platform as Platform,
is_trial_period as Has Started a Trial,
refunded_at as Has Refunded,
is_trial_conversion as Has Converted His Trial,
product_id as ProductId,
purchased_price_in_usd as Purchased Price (USD),

 from {{ source('raw', 'RevenueCatHarmony') }}