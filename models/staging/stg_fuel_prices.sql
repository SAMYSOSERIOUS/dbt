{{ config(materialized='view') }}

SELECT 
    date as period,
    price as fuel_price_usd_per_gallon,
    
    -- Add calculated fields
    LAG(price) OVER (ORDER BY date) as previous_price,
    price - LAG(price) OVER (ORDER BY date) as price_change,
    
    -- Price trend indicators
    CASE 
        WHEN price > LAG(price) OVER (ORDER BY date) THEN 'Increasing'
        WHEN price < LAG(price) OVER (ORDER BY date) THEN 'Decreasing'
        ELSE 'Stable'
    END as price_trend

FROM {{ source('raw', 'fuel_prices') }}
ORDER BY date
