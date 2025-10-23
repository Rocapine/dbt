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
    when upper(substr(campaign_name, 1, 3)) = 'HAR' then 'Harmony'
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
    else 'Unknown'
  end as platform,
  case 
  when ad_account = 'Lifestyle Web' then 'Harmony'
  when ad_account = 'Roca_Unchaind' then 'Unchained'
  when ad_account = 'Unchaind (SGD)' then 'Unchained'
  when ad_account = 'Stashcook Ads' then 'Stashcook'
  when ad_account = 'Push Training' then 'Pushtraining'
  when ad_account = 'Propel (iOS)' then 'Propel'
  when ad_account = 'Propel (Android)' then 'Propel'
  when ad_account = 'Teech ADS' then 'Teech'
  when ad_account = 'Eve: Pre & Post Natal Yoga' then 'Eve'
    else 'Unknown'
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
  when ad_account like '%[Android/iOS] Stashcook%' then 'Stashcook'
  when ad_account like '%[iOS] Pushtraining%' then 'Pushtraining'
  when ad_account like '%[IOS] Unchained%' then 'Unchained'
  when ad_account like '%(Android) Unchained%' then 'Unchained'
  when ad_account like '%[iOS] FitFlow%' then 'FitFlow'
  when ad_account like '%Propel (iOS)%' then 'Propel'
  when ad_account like '%[iOS] Harmony%' then 'Harmony'
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
