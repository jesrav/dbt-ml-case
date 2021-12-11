

{{ config(materialized='table') }}

select "Location",
    "WindGustDir",
    "WindDir3pm",
    "RainToday",
    "RainTomorrow",
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
    
    lag(rainfall, 1, 0) over (partition by location order by location, date) as rainyesterday, 

    CURRENT_TIMESTAMP() as created_at

from {{ ref('stg_weather_aus') }}

