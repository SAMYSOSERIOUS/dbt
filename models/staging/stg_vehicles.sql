{{ config(materialized='view') }}

WITH latest_fuel_price AS (
    SELECT fuel_price_usd_per_gallon
    FROM {{ source('raw', 'fuel_prices') }}
    ORDER BY period DESC
    LIMIT 1
)

SELECT 
    -- Vehicle identification (corrected column names)
    make,
    model,
    year,
    displ as displacement,
    cylinders,
    trany as transmission,  -- Fixed: trany not trans
    drive,
    fuelType1 as primary_fuel_type,  -- Fixed: fuelType1 not fuel_type1
    
    -- MPG metrics
    city08 as city_mpg,
    highway08 as highway_mpg,
    comb08 as combined_mpg_official,
    
    -- Calculate combined MPG (in case comb08 is missing)
    COALESCE(comb08, (city08 + highway08) / 2) as combined_mpg,
    
    -- Calculate derived metrics using latest fuel price
    (SELECT fuel_price_usd_per_gallon FROM latest_fuel_price) as current_fuel_price_usd,
    
    -- Calculate annual fuel cost (15,000 miles per year)
    ROUND((15000 / COALESCE(comb08, (city08 + highway08) / 2)) * 
          (SELECT fuel_price_usd_per_gallon FROM latest_fuel_price), 2) as annual_fuel_cost,
    
    -- Calculate cost per 100 miles
    ROUND((100 / COALESCE(comb08, (city08 + highway08) / 2)) * 
          (SELECT fuel_price_usd_per_gallon FROM latest_fuel_price), 2) as cost_per_100_miles,
    
    -- Convert to L/100km
    ROUND(235.215 / COALESCE(comb08, (city08 + highway08) / 2), 2) as l_per_100km,
    
    -- Create efficiency categories
    CASE 
        WHEN COALESCE(comb08, (city08 + highway08) / 2) >= 40 THEN 'Highly Efficient'
        WHEN COALESCE(comb08, (city08 + highway08) / 2) >= 30 THEN 'Efficient'
        WHEN COALESCE(comb08, (city08 + highway08) / 2) >= 20 THEN 'Moderate'
        ELSE 'Low Efficiency'
    END as efficiency_category,
    
    -- Add processing timestamp
    CURRENT_TIMESTAMP() as processing_timestamp

FROM {{ source('raw', 'vehicles') }}
WHERE city08 > 5 
  AND highway08 > 5 
  AND city08 < 100 
  AND highway08 < 100
  AND displ IS NOT NULL