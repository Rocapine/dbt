{{ config(materialized='table', schema='Core') }}

with asa as (
  select 
  date,
  country,
  campaign_id,
  campaign_name,
  adgroup_id,
  adgroup_name,
  spend as spend_in_currency,
  currency,
  1.00 * spend as spend_in_usd,
  'Apple Search Ads' as channel,
  'IOS' as platform,
  case 
    when upper(substr(campaign_name, 1, 3)) = 'HAR' then 'Harmony'
    else 'Unknown'
  end as ad_account
  from {{ source('ads', 'AsaAds') }}
  where
  upper(substr(campaign_name, 1, 3)) = 'HAR'
),

meta as (
  select 
  date,
  country,
  campaign_id,
  campaign_name,
  adgroup_id,
  adgroup_name,
  spend as spend_in_currency,
  currency,
  case 
  when currency = 'USD' then spend
  when currency = 'EUR' then spend * 1.16
  when currency = 'GBP' then spend * 1.33
  else null
  end as spend_in_usd,
  'Meta' as channel,
  case 
    when upper(campaign_name) like '%IOS%' then 'IOS'
    when upper(campaign_name) like '%AND%' then 'Android'
    else 'Unknown'
  end as platform,
  case 
  when ad_account = 'Lifestyle Web' then 'Harmony'
  when ad_account = 'Roca_Unchaind' then 'Unchained'
  when ad_account = 'Unchaind (SGD)' then 'Unchained'
  when ad_account = 'Stashcook Ads' then 'Stashcook'
  when ad_account = 'Push Training' then 'Pushtraining'
    else 'Unknown'
  end as ad_account
  from {{ source('ads', 'MetaAds') }}
  where
  ad_account like '%Stashcook Ads%'
  or ad_account like '%Push Training%'
  or ad_account like '%Roca_Unchaind%'
  or ad_account like '%Unchaind (SGD)%'
  or ad_account like '%Lifestyle Web%'
),

tiktok as (
  select 
  date,
  country,
  campaign_id,
  campaign_name,
  adgroup_id,
  adgroup_name,
  spend as spend_in_currency,
  currency,
  1.16 * spend as spend_in_usd,
  'Tiktok' as channel,
  case 
  when upper(campaign_name) like '%IOS%' then 'IOS'
  when upper(campaign_name) like '%AND%' then 'Android'
  else null
  end as platform,
  case 
  when ad_account like '%[Android/iOS] Stashcook%' then 'Stashcook'
  when ad_account like '%[iOS] Pushtraining%' then 'Pushtraining'
  when ad_account like '%[IOS] Unchained%' then 'Unchained'
  when ad_account like '%(Android) Unchained%' then 'Unchained'
  when ad_account like '%[iOS] Harmony%' then 'Harmony'
  else 'Unknown'
  end as ad_account
  from {{ source('ads', 'TiktokAds') }}
  where
  ad_account like '%[Android/iOS] Stashcook%'
  or ad_account like '%[iOS] Pushtraining%'
  or ad_account like '%[IOS] Unchained%'
  or ad_account like '%(Android) Unchained%'
  or ad_account like '%[iOS] Harmony%'
),

source as (
  select * from asa
  union all
  select * from meta
  union all
  select * from tiktok
)

select 
date as date,
channel as channel,
country as country,
ad_account as ad_account,
platform as platform,
campaign_id as campaign_id,
campaign_name as campaign_name,
adgroup_id as adgroup_id,
adgroup_name as adgroup_name,
spend_in_currency as spend_in_currency,
currency as currency,
spend_in_usd as spend_in_usd,
from source
