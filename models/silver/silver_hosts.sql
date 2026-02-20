{{ config(materialized='incremental', unique_key='host_id') }}

select 
    host_id,
    replace(host_name, ' ', '_') as host_name,
    IS_superhost as is_superhost,
    host_since,
    response_rate,
    case
        when response_rate > 95 then 'very good'
        when response_rate > 80 then 'good'
        when response_rate > 60 then 'fair'
        else 'poor'
    end as response_rate_quality,
    created_at

from {{ ref('bronze_hosts') }}

{% if is_incremental() %}
where created_at > (select max(created_at) from {{ this }})
{% endif %}


/*

##Comment= Incase if you have late arrival data then this technique we will use

{% if is_incremental() %}
where created_at >= (
    select dateadd(day, -2, max(created_at))
    from {{ this }}
)
{% endif %}

*/