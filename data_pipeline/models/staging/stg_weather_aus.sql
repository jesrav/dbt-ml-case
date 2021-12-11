{{ config(materialized='table') }}

{% set str_columns = [
    "Location",
    "WindGustDir",
    "WindDir3pm",
    "RainToday",
    "RainTomorrow",
    ] 
%}
{% set float_columns = [
    "MinTemp",
    "MaxTemp",
    "Rainfall",
    "Evaporation",    
    "Sunshine",
    "Pressure9am",
    "Pressure3pm",
    "Temp9am",
    "Temp3pm",
    ] 
%}
{% set int_columns = [
    "WindGustSpeed",
    "WindSpeed9am",
    "WindSpeed3pm",
    "Humidity9am",
    "Humidity3pm",
    "Cloud9am",
    "Cloud3pm",
    ] 
%}


with nan_converted_to_null as (
    select 
        Date,
        {% for column in str_columns %}    
            CAST(NULLIF({{ column }}, 'NA') AS VARCHAR) AS {{ column }},    
        {% endfor %}
        {% for column in float_columns %}    
            CAST(NULLIF({{ column }}, 'NA') AS FLOAT) AS {{ column }},    
        {% endfor %}
        {% for column in int_columns %}    
            CAST(NULLIF({{ column }}, 'NA') AS INT) AS {{ column }},    
        {% endfor %}
        sysdate() as created_at
    from {{ source('raw', 'weather_aus') }}
)

select * from nan_converted_to_null
