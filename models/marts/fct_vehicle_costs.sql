{{ config(materialized='table') }}

WITH manufacturer_metrics AS (
    SELECT 
        make,
        efficiency_category,
        COUNT(*) as vehicle_count,
        ROUND(AVG(combined_mpg), 2) as avg_mpg,
        ROUND(AVG(annual_fuel_cost), 2) as avg_annual_cost,
        ROUND(MIN(annual_fuel_cost), 2) as min_annual_cost,
        ROUND(MAX(annual_fuel_cost), 2) as max_annual_cost,
        ROUND(STDDEV(annual_fuel_cost), 2) as cost_std_dev,
        ROUND(AVG(displacement), 2) as avg_displacement,
        COUNT(DISTINCT year) as year_range
    FROM {{ ref('stg_vehicles') }}
    GROUP BY make, efficiency_category
),

cost_rankings AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY avg_annual_cost) as cost_rank,
        ROW_NUMBER() OVER (ORDER BY avg_mpg DESC) as efficiency_rank
    FROM manufacturer_metrics
    WHERE vehicle_count >= 5  -- Only include manufacturers with sufficient data
)

SELECT 
    make,
    efficiency_category,
    vehicle_count,
    avg_mpg,
    avg_annual_cost,
    min_annual_cost,
    max_annual_cost,
    cost_std_dev,
    avg_displacement,
    year_range,
    cost_rank,
    efficiency_rank,
    
    -- Performance tiers
    CASE 
        WHEN cost_rank <= 10 THEN 'Top 10 Most Economical'
        WHEN efficiency_rank <= 10 THEN 'Top 10 Most Efficient'
        WHEN avg_mpg >= 30 THEN 'High Efficiency'
        WHEN avg_annual_cost <= 2000 THEN 'Low Cost'
        ELSE 'Standard'
    END as performance_tier,
    
    -- Analysis metadata
    CURRENT_TIMESTAMP() as analysis_timestamp

FROM cost_rankings
ORDER BY avg_annual_cost