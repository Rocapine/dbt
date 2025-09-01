{{ config(materialized='table') }}

select * except (grace_period_end_time) from {{ source('raw', 'RevenueCatHarmonyTer') }}
union all
select * except (grace_period_end_time) from {{ source('raw', 'RevenueCatUnchained') }}





