{% set nights_booked= 1 %}



select *
from {{ref ('bronze_bookings')}}
{%if nights_booked == 1 %}
where  NIGHTS_BOOKED >{{nights_booked}}
{% else %}
where NIGHTS_BOOKED = {{nights_booked}}
{% endif %}


