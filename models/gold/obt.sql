{{ config(materialized='incremental', unique_key='booking_id') }}

{#- Define configs as you already did -#}
{% set configs = [
    {
        "table" : "AIRBNB.SILVER.SILVER_BOOKINGS",
        "columns" : "SILVER_bookings.*",
        "alias" : "SILVER_bookings"
    },
    { 
        "table" : "AIRBNB.SILVER.SILVER_LISTINGS",
        "columns" : "SILVER_listings.HOST_ID, SILVER_listings.PROPERTY_TYPE, SILVER_listings.ROOM_TYPE, SILVER_listings.CITY, SILVER_listings.COUNTRY, SILVER_listings.ACCOMMODATES, SILVER_listings.BEDROOMS, SILVER_listings.BATHROOMS, SILVER_listings.PRICE_PER_NIGHT, SILVER_listings.PRICE_PER_NIGHT_TAG, SILVER_listings.CREATED_AT AS LISTING_CREATED_AT",
        "alias" : "SILVER_listings",
        "join_condition" : "SILVER_bookings.listing_id = SILVER_listings.listing_id"
    },
    {
        "table" : "AIRBNB.SILVER.SILVER_HOSTS",
        "columns" : "SILVER_hosts.HOST_NAME, SILVER_hosts.HOST_SINCE, SILVER_hosts.IS_SUPERHOST, SILVER_hosts.RESPONSE_RATE, SILVER_hosts.RESPONSE_RATE_QUALITY, SILVER_hosts.CREATED_AT AS HOST_CREATED_AT",
        "alias" : "SILVER_hosts",
        "join_condition" : "SILVER_listings.host_id = SILVER_hosts.host_id"
    }
] %}

{#- Build the SELECT columns dynamically -#}
select
{% for table in configs %}
    {{ table.columns }}{% if not loop.last %},{% endif %}
{% endfor %}
from {{ ref('silver_bookings') }} as SILVER_bookings

{#- Apply joins dynamically (skip first table which is base) -#}
{% for table in configs[1:] %}
left join {{ table.table }} as {{ table.alias }}
    on {{ table.join_condition }}
{% endfor %}
/*
Comment = For handling late arrival data back of 2 days ago 
*/
{% if is_incremental() %}
where SILVER_bookings.created_at >= (select dateadd(day, -2, max(created_at)) from {{ this }})
{% endif %}