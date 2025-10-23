{{ config(materialized='table', schema='Core') }}

with ios as (
  select
    TransactionId,
    first_transaction_date,
    has_started_trial,
    has_converted_trial,
    has_direct_purchase,
    has_paid,
    country,
    product_id,
    bundle_id,
    app_apple_id,
    customer_price_in_currency,
    currency,
    customer_price_in_usd,
    expected_proceeds_in_currency,
    expected_proceeds_in_usd,
    duration,
    platform,
    app_name
  from {{ ref('Transactions_appstoreconnect') }}
),
android as (
  select
    TransactionId,
    first_transaction_date,
    has_started_trial,
    has_converted_trial,
    has_direct_purchase,
    has_paid,
    country,
    product_id,
    bundle_id,
    cast(null as string) as app_apple_id,
    customer_price_in_currency,
    currency,
    customer_price_in_usd,
    expected_proceeds_in_currency,
    expected_proceeds_in_usd,
    duration,
    platform,
    app_name
  from {{ ref('Transactions_googleplay') }}
)

select * from ios
union all
select * from android


