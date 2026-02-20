
{{ config( materialized='incremental'  ) }} 

select * from {{ source('staging','bookings') }} 

{% if is_incremental() %} 
    where created_at > ( select coalesce(max(created_at), '1900-01-01') from {{ this }} ) 
{% endif %}
 


/* 
In case if you want to handle late arrival data then this function will work

{% set lookback_days = 1 %}  -- look back 1 day
{{ config(materialized='incremental', unique_key='booking_id') }}

select *
from {{ source('staging','bookings') }} s
{% if is_incremental() %}
where created_at > dateadd(day, -{{ lookback_days }}, current_timestamp())
{% endif %}


*/

