
{{ config(materialized='view') }}

{% set columns_to_lag = [
    "MinTemp",
    "MaxTemp",
    "Rainfall",
    "Evaporation",    
    "Sunshine",
    "Pressure9am",
    "Pressure3pm",
    "Temp9am",
    "Temp3pm",
    "WindGustSpeed",
    "WindSpeed9am",
    "WindSpeed3pm",
    "Humidity9am",
    "Humidity3pm",
    "Cloud9am",
    "Cloud3pm",
    ] 
%}

with lagged_features_added as (
    select 
        {% for column in columns_to_lag %}    
            lag({{ column }}, 1, 0) over (partition by location order by location, date) as {{ column }}_yesterday, 
        {% endfor %}

        Location,
        WindGustDir,
        WindDir3pm,
        RainToday,
        RainTomorrow,
        MinTemp,
        MaxTemp,
        Rainfall,
        Evaporation,    
        Sunshine,
        Pressure9am,
        Pressure3pm,
        Temp9am,
        Temp3pm,
        WindGustSpeed,
        WindSpeed9am,
        WindSpeed3pm,
        Humidity9am,
        Humidity3pm,
        Cloud9am,
        Cloud3pm, 
        
        sysdate() as created_at

    from {{ ref('stg_weather_aus') }}
)

select * from lagged_features_added