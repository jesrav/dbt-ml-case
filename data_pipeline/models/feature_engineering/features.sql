

{{ config(materialized='table') }}

select *
from {{ source('ml_schema', 'concrete') }}

