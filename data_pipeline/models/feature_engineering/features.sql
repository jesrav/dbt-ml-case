

{{ config(materialized='table') }}

select *
from {{ ref('stg_weather_aus') }}

