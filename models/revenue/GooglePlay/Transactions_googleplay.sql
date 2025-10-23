{{ config(materialized='table', alias='Transactions') }}

with source as (
  select * from {{ source('googleplay', 'Notification') }}
),
normalized_orders as (
  select
    source.*,
    REGEXP_REPLACE(source.latestOrderId, '\\.\\.\\d+$', '') as base_order_id,
    CASE
      WHEN source.basePlanId IS NOT NULL AND source.basePlanId != ''
        THEN CONCAT(CAST(source.productId AS STRING), ':', CAST(source.basePlanId AS STRING))
      ELSE CAST(source.productId AS STRING)
    END as product_id
  from source
),
cohort as (
  select base_order_id
  from normalized_orders
  group by base_order_id
),  
trials as (
  select distinct normalized_orders.base_order_id
  from normalized_orders   
  join cohort using (base_order_id)
  where normalized_orders.latestOrderId = normalized_orders.base_order_id
  AND normalized_orders.subscriptionState = 'SUBSCRIPTION_STATE_ACTIVE'
  AND normalized_orders.offerPhase = 'FREE_TRIAL'
),
converted as (
  select distinct normalized_orders.base_order_id
  from normalized_orders
  join trials using (base_order_id)
  WHERE REGEXP_CONTAINS(normalized_orders.latestOrderId, '\\.\\.0$')
  AND normalized_orders.subscriptionState = 'SUBSCRIPTION_STATE_ACTIVE'
  AND normalized_orders.offerPhase = 'FREE_TRIAL'
),
direct_purchases as (
  select distinct normalized_orders.base_order_id
  from normalized_orders 
  where normalized_orders.latestOrderId = normalized_orders.base_order_id
  AND normalized_orders.subscriptionState = 'SUBSCRIPTION_STATE_ACTIVE'
  AND normalized_orders.offerPhase = 'BASE' 
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
  normalized_orders.base_order_id as TransactionId,
  min(date(case when normalized_orders.latestOrderId = normalized_orders.base_order_id then normalized_orders.startTime end)) as first_transaction_date,
  max(case when trials.base_order_id is not null then true else false end) as has_started_trial,
  max(case when converted.base_order_id is not null then true else false end) as has_converted_trial,
  max(case when direct_purchases.base_order_id is not null then true else false end) as has_direct_purchase,
  any_value(normalized_orders.regionCode) as country,
  any_value(normalized_orders.product_id) as product_id,
  any_value(normalized_orders.packageName) as bundle_id,
  any_value(products.customer_price_in_currency) as customer_price_in_currency,
  any_value(normalized_orders.currencyCode) as currency,
  any_value(products.customer_price_in_usd) as customer_price_in_usd,
  any_value(products.expected_proceeds_in_currency) as expected_proceeds_in_currency,
  any_value(products.expected_proceeds_in_usd) as expected_proceeds_in_usd,
  any_value(products.duration) as duration,
  "Android" as platform,
  any_value(case
    when normalized_orders.packageName = 'com.rocapine.harmony' then 'Harmony'
    when normalized_orders.packageName = 'com.applostudio.Unchaind' then 'Unchained'
    when normalized_orders.packageName = 'com.stashbox.stashcook' then 'Stashcook'
    when normalized_orders.packageName = 'com.albernackee.blur' then 'Pushtraining'
    else null
  end) as app_name
from normalized_orders 
left join trials using (base_order_id)
left join converted using (base_order_id)
left join direct_purchases using (base_order_id)
left join products 
  on lower(trim(normalized_orders.regionCode)) = lower(trim(
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
  and normalized_orders.product_id = products.product_SKU

group by normalized_orders.base_order_id


