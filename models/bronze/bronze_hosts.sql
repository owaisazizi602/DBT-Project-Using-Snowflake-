

{{ config( materialized='incremental'  ) }} 

select * from {{ source('staging','hosts') }} 

{% if is_incremental() %} 
    where created_at > ( select coalesce(max(created_at), '1900-01-01') from {{ this }} ) 
{% endif %}



/*


incase you want to handle late arrival data depending upon the days
{% set lookback_days = 1 %}  -- look back 1 day
{{ config(materialized='incremental', unique_key='host_id') }}

select *
from {{ source('staging','hosts') }} s
{% if is_incremental() %}
where created_at > dateadd(day, -{{ lookback_days }}, current_timestamp())
{% endif %}

*/