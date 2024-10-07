


{{ config(materialized='table') }}

with date_spine as (

    {% if target.type == 'postgres' %}
        {{ dbt_utils.date_spine("day", "'2018-01-01'::date", "'2018-01-10'::date") }}
        
    {% elif target.type == 'bigquery' %}
        select cast(date_day as date) as date_day
        from ({{ dbt_utils.date_spine("day", "'2018-01-01'", "'2018-01-10'") }})
    
    {% else %}
        {{ dbt_utils.date_spine("day", "'2018-01-01'", "'2018-01-10'") }}
    {% endif %}

)

select date_day
from date_spine

