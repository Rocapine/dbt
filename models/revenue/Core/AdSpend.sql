{{ config(materialized='table', schema='Core') }}

with asa as (
  select 
  date,
  country,
  campaign_id,
  campaign_name,
  adgroup_id,
  adgroup_name,
  spend,
  currency,
  'Apple Search Ads' as channel,
  'IOS' as platform,
  case 
    when upper(substr(campaign_name, 1, 3)) = 'HAR' then 'harmony'
    when upper(substr(campaign_name, 1, 3)) = 'CBT' then 'CBT'
    else ad_account
  end as ad_account
  from {{ source('ads', 'AsaAds') }}
),

meta as (
  select 
  date,
  country,
  campaign_id,
  campaign_name,
  adgroup_id,
  adgroup_name,
  spend,
  currency,
  'Meta' as channel,
  case 
    when upper(campaign_name) like '%IOS%' then 'IOS'
    when upper(campaign_name) like '%AND%' then 'Android'
    else null
  end as platform,
  case 
  when campaign_name like '%Lifestyle_web%' then 'Harmony'
  when campaign_name like '%Roca_Unchaind%'  then 'Unchained'
  when campaign_name like '%Unchaind (SGD)%' then 'Unchained'
  when campaign_name like '%Stashcook Ads%' then 'Stashcook'
  when campaign_name like '%Push Training%' then 'Pushtraining'
  when campaign_name like '%Propel (iOS)%' then 'Propel'
  when campaign_name like '%Propel (Android)%' then 'Propel'
  when campaign_name like '%Teech_ADS%' then 'Teech'
  when campaign_name like '%Eve:_Pre_&_Post_Natal_Yoga%' then 'Eve'
  when campaign_name like '%Lifestyle_Web%' then 'Lifestyle'
    else ad_account
  end as ad_account
  from {{ source('ads', 'MetaAds') }}
),

tiktok as (
  select 
  date,
  country,
  campaign_id,
  campaign_name,
  adgroup_id,
  adgroup_name,
  spend,
  currency,
  'Tiktok' as channel,
  case 
  when upper(campaign_name) like '%IOS%' then 'IOS'
  when upper(campaign_name) like '%AND%' then 'Android'
  else null
  end as platform,
  case 
  when campaign_name like '%[Android/iOS] Stashcook%' then 'Stashcook'
  when campaign_name like '%[iOS] Pushtraining%' then 'Pushtraining'
  when campaign_name like '%[IOS] Unchained%' then 'Unchained'
  when campaign_name like '%(Android) Unchained%' then 'Unchained'
  when campaign_name like '%[iOS] FitFlow%' then 'FitFlow'
  when campaign_name like '%Propel (iOS)%' then 'Propel'
  when campaign_name like '%[iOS] Harmony%' then 'Harmony'
  else 'Unknown'
  end as ad_account
  from {{ source('ads', 'TiktokAds') }}
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
spend as spend,
currency as currency,
from source
