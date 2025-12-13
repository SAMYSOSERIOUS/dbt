{{ config(materialized='view') }}

SELECT 
    period,
    fuel_price_usd_per_gallon,
    
    -- Add calculated fields
    LAG(fuel_price_usd_per_gallon) OVER (ORDER BY period) as previous_price,
    fuel_price_usd_per_gallon - LAG(fuel_price_usd_per_gallon) OVER (ORDER BY period) as price_change,
    
    -- Price trend indicators
    CASE 
        WHEN fuel_price_usd_per_gallon > LAG(fuel_price_usd_per_gallon) OVER (ORDER BY period) THEN 'Increasing'
        WHEN fuel_price_usd_per_gallon < LAG(fuel_price_usd_per_gallon) OVER (ORDER BY period) THEN 'Decreasing'
        ELSE 'Stable'
    END as price_trend

FROM {{ source('raw', 'fuel_prices') }}
ORDER BY period