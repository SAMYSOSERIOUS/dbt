{{ config(materialized='table') }}

-- Star Schema Fact Table: Vehicle Costs
-- Links dimension tables via foreign keys

WITH latest_fuel_price AS (
    SELECT fuel_price_usd_per_gallon
    FROM {{ ref('stg_fuel_prices') }}
    ORDER BY period DESC
    LIMIT 1
),

vehicle_costs AS (
    SELECT 
        v.vehicle_key,
        m.manufacturer_key,
        v.make,
        v.model,
        v.year,
        v.vehicle_class,
        s.combined_mpg,
        s.city_mpg,
        s.highway_mpg,
        s.displacement,
        s.efficiency_category,
        
        -- Calculate costs using latest fuel price
        ROUND((15000 / s.combined_mpg) * f.fuel_price_usd_per_gallon, 2) as annual_fuel_cost,
        ROUND((100 / s.combined_mpg) * f.fuel_price_usd_per_gallon, 2) as cost_per_100_miles,
        f.fuel_price_usd_per_gallon as current_fuel_price
        
    FROM {{ ref('stg_vehicles') }} s
    CROSS JOIN latest_fuel_price f
    LEFT JOIN {{ ref('dim_vehicles') }} v 
        ON s.make = v.make 
        AND s.model = v.model 
        AND s.year = v.year
    LEFT JOIN {{ ref('dim_manufacturers') }} m 
        ON s.make = m.manufacturer_name
)

SELECT 
    -- Dimension Keys (Foreign Keys)
    vehicle_key,
    manufacturer_key,
    
    -- Attributes
    make,
    model,
    year,
    
    -- Metrics
    combined_mpg,
    city_mpg,
    highway_mpg,
    annual_fuel_cost,
    cost_per_100_miles,
    current_fuel_price,
    
    -- Performance Tiers
    CASE 
        WHEN combined_mpg >= 40 THEN 'Excellent Efficiency'
        WHEN combined_mpg >= 30 THEN 'High Efficiency'
        WHEN combined_mpg >= 20 THEN 'Average Efficiency'
        ELSE 'Low Efficiency'
    END as efficiency_tier,
    
    CASE 
        WHEN annual_fuel_cost <= 1500 THEN 'Very Low Cost'
        WHEN annual_fuel_cost <= 2000 THEN 'Low Cost'
        WHEN annual_fuel_cost <= 2500 THEN 'Medium Cost'
        WHEN annual_fuel_cost <= 3000 THEN 'High Cost'
        ELSE 'Very High Cost'
    END as cost_tier

FROM vehicle_costs
WHERE vehicle_key IS NOT NULL AND manufacturer_key IS NOT NULL
ORDER BY annual_fuel_cost