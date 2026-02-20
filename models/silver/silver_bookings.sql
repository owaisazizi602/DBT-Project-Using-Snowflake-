{{ config(materialized='incremental', unique_key='booking_id') }}

select
    booking_id,
    listing_id,
    booking_date,
    {{ multiply('nights_booked', 'booking_amount', 2) }} as total_amount,
    cleaning_fee,
    service_fee,
    booking_status,
    created_at
from {{ ref('bronze_bookings') }}

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