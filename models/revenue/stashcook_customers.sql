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
  select distinct source.`Q user id`
  from source   
  join cohort using (`Q user id`)
  where source.`event name` = 'Trial Started'
),
converted as (
  select distinct source.`Q user id`
  from source
  join trials as tr using (`Q user id`)
  group by source.`Q user id`
  having countif(source.`event name` = 'Trial Converted') > 0
  and countif(source.`event name` = 'Trial Canceled') = 0
  and countif(source.`event name` = 'Trial Expired') = 0
  and countif(coalesce(cast(source.`Refund` as string), '') != '') = 0
),
base as (
  select
    `Q user id`,
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
    `Country`
  from source
)

select
  base.`Q user id` as Qonversion_id,
  min(date(base.`Event Receive Date`)) as first_seen_date,
  any_value(base.`App Name`) as app_name,
  any_value(base.`Country`) as country,
  any_value(base.`Platform`) as platform,
  max(base.`Refund`) as has_refunded,
  max(case when tr.`Q user id` is not null then true else false end) as has_started_trial,
  max(case when converted.`Q user id` is not null then true else false end) as has_converted_trial,
  any_value(base.`Product ID`) as product_id,
  max(base.`Price Usd`) as purchased_price_usd,
  any_value(base.`Proceeds Usd`) as customer_price_in_usd,
  any_value(base.`Proceeds`) as expected_proceeds_in_currency,
  any_value(base.`Proceeds Usd`) as expected_proceeds_in_usd,
  case
    when lower(any_value(base.`Country`)) in ('al','at','by','be','ba','bg','hr','cz','dk','de','ee','fi','gr','hu','is','ie','it','lt','lv','lu','mk','mt','md','nl','no','pl','pt','ro','ru','rs','sk','si','es','se','ch','tr','ua','cy') then 'EUROPE'
    when lower(any_value(base.`Country`)) in ('us','ca') then 'US/CA'
    else 'Rest of The world'
  end as region
from base 
left join trials tr on tr.`Q user id` = base.`Q user id`
left join converted on converted.`Q user id` = base.`Q user id`
group by base.`Q user id`