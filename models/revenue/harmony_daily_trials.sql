{{ config(materialized='table') }}

select
  date(first_seen_time) as first_seen_date,
  count(distinct if (is_trial_period, rc_original_app_user_id, null)) as trial_starts,
  count(distinct if(is_trial_conversion and unsubscribe_detected_at is null, rc_original_app_user_id, null)) as converted_users
from {{ source('raw', 'RevenueCatHarmony') }}
group by 1
order by 1


