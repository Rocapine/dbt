{{ config(materialized='table') }}

with source as (
  select *  except (`Transaction ID`,`Subscription Group`,`App ID`) from {{ source('raw', 'QonversionStashcookAndroid') }} 
  union all 
  select *  except (`Transaction ID`,`Subscription Group`,`App ID`) from {{ source('raw', 'QonversionStashcookIOS') }}
),
--transaction id is considered as integer in one table and string in another table, but we don't need it 
--so we exclude it from the source
cohort as (
  select `Q user id`
  from source
  group by `Q user id`
), 
trials as (
  select distinct source.`Q user id`, source.`Event Receive Date` as trial_start_date
  from source   
  join cohort using (`Q user id`)
  where source.`event name` = 'Trial Started'
),
converted as (
  select distinct source.`Q user id`, source.`Event Receive Date` as trial_converted_date
  from source
  join trials as tr using (`Q user id`)
  group by source.`Q user id`, source.`Event Receive Date`
  having countif(source.`event name` = 'Trial Converted') > 0
  and countif(source.`event name` = 'Trial Canceled') = 0
  and countif(source.`event name` = 'Trial Expired') = 0
  and countif(coalesce(cast(source.`Refund` as string), '') != '') = 0
),
cancels as (
  select `Q user id`, `Event Receive Date` as cancel_date
  from source
  where `event name` = 'Subscription Canceled'
),
direct_subscription as (
  select source.`Q user id`, source.`Event Receive Date` as direct_subscription_date
  from source 
  where source.`event name` = 'Subscription Started'
  
),
base as (
  select
    `Q user id`,
    `Install Date`,
    `Event Receive Date`,
    `event name`,
    `App Name`,
    `Platform`,
    `Product ID`,
    `Price`,
    `Proceeds`,
    `Price Usd`,
    `Proceeds Usd`,
    `Refund`,
    `Currency`,
    `Country`
  from source
),
agg as (
  select
    base.`Q user id` as Qonversion_id,
    min(date(base.`Install Date`)) as first_seen_date,
    any_value(base.`App Name`) as app_name,
    any_value(base.`Country`) as country,
    any_value(base.`Platform`) as platform,
    max(base.`Refund`) as has_refunded,
    max(case when tr.`Q user id` is not null then true else false end) as has_started_trial,
    max(case when tr.`trial_start_date` is not null then tr.`trial_start_date` else null end) as has_started_trial_date,
    max(case when converted.`Q user id` is not null then true else false end) as has_converted_trial,
    max(case when converted.`trial_converted_date` is not null then converted.`trial_converted_date` else null end) as has_converted_trial_date,
    max(case when direct_subscription.`Q user id` is not null then true else false end) as has_started_direct_subscription,
    max(case when direct_subscription.`direct_subscription_date` is not null then direct_subscription.`direct_subscription_date` else null end) as has_started_direct_subscription_date,
    ARRAY_AGG(base.`Product ID` ORDER BY base.`Event Receive Date` DESC LIMIT 1)[OFFSET(0)] as product_id,
    any_value(base.`Currency`) as currency,
    max(safe_cast(base.`Price Usd` as float64)) as customer_price_in_usd,
    max(safe_cast(base.`Proceeds Usd` as float64)) as expected_proceeds_in_usd,
    max(safe_cast(base.`Price` as float64)) as customer_price_in_currency,
    max(safe_cast(base.`Proceeds` as float64)) as expected_proceeds_in_currency,
    case
      when lower(ARRAY_AGG(base.`Product ID` ORDER BY base.`Event Receive Date` DESC LIMIT 1)[OFFSET(0)]) like '%lifetime%' then 'Lifetime'
      when lower(ARRAY_AGG(base.`Product ID` ORDER BY base.`Event Receive Date` DESC LIMIT 1)[OFFSET(0)]) like '%year%' then '1 year'
      when lower(ARRAY_AGG(base.`Product ID` ORDER BY base.`Event Receive Date` DESC LIMIT 1)[OFFSET(0)]) like '%premium%' then '1 month'
      when lower(ARRAY_AGG(base.`Product ID` ORDER BY base.`Event Receive Date` DESC LIMIT 1)[OFFSET(0)]) like '%month%' then '1 month'

      else null
    end as duration,
    case
      when lower(any_value(base.`Country`)) in ('ad','al','at','by','be','ba','bg','hr','cz','dk','de','ee','fi','gr','fr','gb','hu','is','ie','it','lt','lv','lu','mk','mt','md','nl','no','pl','pt','ro','ru','rs','sk','si','es','se','ch','tr','ua','cy') then 'EUROPE'
      when lower(any_value(base.`Country`)) in ('us','ca') then 'US/CA'
      else 'Rest of The world'
    end as region
  from base 
  left join trials tr on tr.`Q user id` = base.`Q user id`
  left join converted on converted.`Q user id` = base.`Q user id`
  left join direct_subscription on direct_subscription.`Q user id` = base.`Q user id`
  group by base.`Q user id`
)

select
  Qonversion_id,
  first_seen_date,
  app_name,
  country,
  platform,
  has_refunded,
  has_started_trial,
  has_started_trial_date,
  case
    when has_converted_trial_date is not null and (has_started_direct_subscription_date is null or has_converted_trial_date < has_started_direct_subscription_date) then true
    else false
  end as has_converted_trial,
  case
    when has_started_direct_subscription_date is not null and (has_converted_trial_date is null or has_started_direct_subscription_date <= has_converted_trial_date) then true
    else false
  end as has_started_direct_subscription,
  case
  when has_converted_trial_date is not null and has_started_direct_subscription_date is not null then least(has_converted_trial_date, has_started_direct_subscription_date)
  when has_converted_trial_date is not null then has_converted_trial_date
  when has_started_direct_subscription_date is not null then has_started_direct_subscription_date
    else null
  end as user_activated_at,

  product_id,
  currency,
  customer_price_in_usd,
  expected_proceeds_in_usd,
  customer_price_in_currency,
  expected_proceeds_in_currency,
  duration,
  case
    when lower(country) in ('al','at','by','be','ba','bg','hr','cz','dk','de','ee','fi','gr','hu','is','ie','it','lt','lv','lu','mk','mt','md','nl','no','pl','pt','ro','ru','rs','sk','si','es','se','ch','tr','ua','cy') then 'EUROPE'
    when lower(country) in ('us','ca') then 'US/CA'
    else 'Rest of The world'
  end as region,
from agg