{{ config(materialized='table',schema='Experimentation') }}

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
converted_revenuecat as (
  select distinct source.rc_original_app_user_id
  from source
  join trials as tr using (rc_original_app_user_id)
  WHERE source.is_trial_conversion = TRUE
  AND source.refunded_at IS NULL  
),
converted_rocapine as (
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
  from {{ source('core', 'Products') }}
)

select
  base.rc_original_app_user_id as revenuecat_id,
  min(date(base.first_seen_time)) as first_seen_date,
  any_value(base.country) as country,
  any_value(base.platform) as platform,
  max(base.refunded_at) as has_refunded,
  max(case when tr.rc_original_app_user_id is not null then true else false end) as has_started_trial,
  max(case when converted_revenuecat.rc_original_app_user_id is not null then true else false end) as has_converted_trial_revenuecat,
  max(case when converted_rocapine.rc_original_app_user_id is not null then true else false end) as has_converted_trial_rocapine,
  any_value(base.product_identifier) as product_id,
  max(base.purchase_price_in_usd) as purchased_price_usd,
  any_value(products.customer_price_in_currency) as customer_price_in_currency,
  any_value(products.customer_price_in_usd) as customer_price_in_usd,
  any_value(products.expected_proceeds_in_currency) as expected_proceeds_in_currency,
  any_value(products.expected_proceeds_in_usd) as expected_proceeds_in_usd,
  any_value(products.duration) as duration,
  case
    when lower(any_value(base.country)) in ('ad','al','at','by','be','ba','bg','hr','cz','dk','de','ee','fi','gr','fr','gb','hu','is','ie','it','lt','lv','lu','mk','mt','md','nl','no','pl','pt','ro','ru','rs','sk','si','es','se','ch','tr','ua','cy') then 'EUROPE'
    when lower(any_value(base.country)) in ('us','ca') then 'US/CA'
    else 'Rest of The world'
  end as region
from base 
left join trials tr on tr.rc_original_app_user_id = base.rc_original_app_user_id
left join converted_revenuecat on converted_revenuecat.rc_original_app_user_id = base.rc_original_app_user_id
left join converted_rocapine on converted_rocapine.rc_original_app_user_id = base.rc_original_app_user_id
left join products 
  on lower(trim(base.country)) = lower(trim(
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
  and base.product_identifier = products.product_SKU

group by base.rc_original_app_user_id
