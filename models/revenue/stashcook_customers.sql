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
products as (
  select
    country,
    product_SKU,
    customer_price_in_currency,
    customer_price_in_usd,
    expected_proceeds_in_currency,
    expected_proceeds_in_usd,
    duration
  from {{ source('raw', 'Products') }}
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
  agg.Qonversion_id,
  first_seen_date,
  app_name,
  agg.country,
  agg.platform,
  agg.has_refunded,
  agg.has_started_trial,
  agg.has_started_trial_date,
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
  case when agg.platform = 'iOS' then products.customer_price_in_usd else agg.customer_price_in_usd end as customer_price_in_usd,
  case when agg.platform = 'iOS' then products.expected_proceeds_in_usd else agg.expected_proceeds_in_usd end as expected_proceeds_in_usd,
  case when agg.platform = 'iOS' then products.customer_price_in_currency else agg.customer_price_in_currency end as customer_price_in_currency,
  case when agg.platform = 'iOS' then products.expected_proceeds_in_currency else agg.expected_proceeds_in_currency end as expected_proceeds_in_currency,
  agg.duration,
  case
    when lower(agg.country) in ('al','at','by','be','ba','bg','hr','cz','dk','de','ee','fi','gr','hu','is','ie','it','lt','lv','lu','mk','mt','md','nl','no','pl','pt','ro','ru','rs','sk','si','es','se','ch','tr','ua','cy') then 'EUROPE'
    when lower(agg.country) in ('us','ca') then 'US/CA'
    else 'Rest of The world'
  end as region
  from agg
  left join products 
  on lower(trim(agg.country)) = lower(trim(
    case 
      when length(products.country) = 3 then 
        case upper(products.country)
          when 'ARE' then 'AE' when 'AFG' then 'AF' when 'ALB' then 'AL' when 'ARM' then 'AM' when 'AND' then 'AD'
          when 'AGO' then 'AO' when 'ARG' then 'AR' when 'AUT' then 'AT' when 'AUS' then 'AU' when 'ABW' then 'AW'
          when 'AZE' then 'AZ' when 'BIH' then 'BA' when 'BRB' then 'BB' when 'BGD' then 'BD' when 'BEL' then 'BE'
          when 'BFA' then 'BF' when 'BGR' then 'BG' when 'BHR' then 'BH' when 'BDI' then 'BI' when 'BEN' then 'BJ'
          when 'BMU' then 'BM' when 'BRN' then 'BN' when 'BOL' then 'BO' when 'BRA' then 'BR' when 'BHS' then 'BS'
          when 'BTN' then 'BT' when 'BWA' then 'BW' when 'BLR' then 'BY' when 'BLZ' then 'BZ' when 'CAN' then 'CA'
          when 'COD' then 'CD' when 'CAF' then 'CF' when 'COG' then 'CG' when 'CHE' then 'CH' when 'CIV' then 'CI'
          when 'COK' then 'CK' when 'CHL' then 'CL' when 'CMR' then 'CM' when 'CHN' then 'CN' when 'COL' then 'CO'
          when 'CRI' then 'CR' when 'CUB' then 'CU' when 'CPV' then 'CV' when 'CYP' then 'CY' when 'CZE' then 'CZ'
          when 'DEU' then 'DE' when 'DJI' then 'DJ' when 'DNK' then 'DK' when 'DOM' then 'DO' when 'DZA' then 'DZ'
          when 'ECU' then 'EC' when 'EST' then 'EE' when 'EGY' then 'EG' when 'ERI' then 'ER' when 'ESP' then 'ES'
          when 'ETH' then 'ET' when 'FIN' then 'FI' when 'FJI' then 'FJ' when 'FSM' then 'FM' when 'FRA' then 'FR'
          when 'GAB' then 'GA' when 'GBR' then 'GB' when 'GRD' then 'GD' when 'GEO' then 'GE' when 'GHA' then 'GH'
          when 'GMB' then 'GM' when 'GIN' then 'GN' when 'GNQ' then 'GQ' when 'GRC' then 'GR' when 'GTM' then 'GT'
          when 'GNB' then 'GW' when 'GUY' then 'GY' when 'HKG' then 'HK' when 'HND' then 'HN' when 'HRV' then 'HR'
          when 'HTI' then 'HT' when 'HUN' then 'HU' when 'IDN' then 'ID' when 'IRL' then 'IE' when 'ISR' then 'IL'
          when 'IND' then 'IN' when 'IRQ' then 'IQ' when 'IRN' then 'IR' when 'ISL' then 'IS' when 'ITA' then 'IT'
          when 'JAM' then 'JM' when 'JOR' then 'JO' when 'JPN' then 'JP' when 'KEN' then 'KE' when 'KGZ' then 'KG'
          when 'KHM' then 'KH' when 'KIR' then 'KI' when 'COM' then 'KM' when 'KNA' then 'KN' when 'PRK' then 'KP'
          when 'KOR' then 'KR' when 'KWT' then 'KW' when 'KAZ' then 'KZ' when 'LAO' then 'LA' when 'LBN' then 'LB'
          when 'LCA' then 'LC' when 'LIE' then 'LI' when 'LKA' then 'LK' when 'LBR' then 'LR' when 'LSO' then 'LS'
          when 'LTU' then 'LT' when 'LUX' then 'LU' when 'LVA' then 'LV' when 'LBY' then 'LY' when 'MAR' then 'MA'
          when 'MCO' then 'MC' when 'MDA' then 'MD' when 'MNE' then 'ME' when 'MDG' then 'MG' when 'MKD' then 'MK'
          when 'MLI' then 'ML' when 'MMR' then 'MM' when 'MNG' then 'MN' when 'MAC' then 'MO' when 'MRT' then 'MR'
          when 'MLT' then 'MT' when 'MUS' then 'MU' when 'MDV' then 'MV' when 'MWI' then 'MW' when 'MEX' then 'MX'
          when 'MYS' then 'MY' when 'MOZ' then 'MZ' when 'NAM' then 'NA' when 'NER' then 'NE' when 'NGA' then 'NG'
          when 'NIC' then 'NI' when 'NLD' then 'NL' when 'NOR' then 'NO' when 'NPL' then 'NP' when 'NRU' then 'NR'
          when 'NZL' then 'NZ' when 'OMN' then 'OM' when 'PAN' then 'PA' when 'PER' then 'PE' when 'PYF' then 'PF'
          when 'PNG' then 'PG' when 'PHL' then 'PH' when 'PAK' then 'PK' when 'POL' then 'PL' when 'PRT' then 'PT'
          when 'PRY' then 'PY' when 'QAT' then 'QA' when 'ROU' then 'RO' when 'SRB' then 'RS' when 'RUS' then 'RU'
          when 'RWA' then 'RW' when 'SAU' then 'SA' when 'SLB' then 'SB' when 'SYC' then 'SC' when 'SDN' then 'SD'
          when 'SWE' then 'SE' when 'SGP' then 'SG' when 'SVN' then 'SI' when 'SVK' then 'SK' when 'SLE' then 'SL'
          when 'SMR' then 'SM' when 'SEN' then 'SN' when 'SOM' then 'SO' when 'SUR' then 'SR' when 'SSD' then 'SS'
          when 'STP' then 'ST' when 'SLV' then 'SV' when 'SYR' then 'SY' when 'SWZ' then 'SZ' when 'TCA' then 'TC'
          when 'TCD' then 'TD' when 'TGO' then 'TG' when 'THA' then 'TH' when 'TJK' then 'TJ' when 'TKM' then 'TM'
          when 'TUN' then 'TN' when 'TON' then 'TO' when 'TUR' then 'TR' when 'TTO' then 'TT' when 'TUV' then 'TV'
          when 'TWN' then 'TW' when 'TZA' then 'TZ' when 'UKR' then 'UA' when 'UGA' then 'UG' when 'USA' then 'US'
          when 'URY' then 'UY' when 'UZB' then 'UZ' when 'VAT' then 'VA' when 'VCT' then 'VC' when 'VEN' then 'VE'
          when 'VGB' then 'VG' when 'VIR' then 'VI' when 'VNM' then 'VN' when 'VUT' then 'VU' when 'WLF' then 'WF'
          when 'WSM' then 'WS' when 'YEM' then 'YE' when 'MYT' then 'YT' when 'ZAF' then 'ZA' when 'ZMB' then 'ZM'
          when 'ZWE' then 'ZW' when 'CYM' then 'KY' else substr(products.country, 1, 2)
        end
      else products.country 
    end))
  and agg.product_id = products.product_SKU
