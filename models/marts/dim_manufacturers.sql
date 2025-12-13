{{ config(materialized='table') }}

WITH manufacturer_stats AS (
    SELECT 
        make,
        COUNT(*) as total_models,
        MIN(year) as first_year,
        MAX(year) as latest_year
    FROM {{ ref('stg_vehicles') }}
    GROUP BY make
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY make) as manufacturer_key,
    make as manufacturer_name,
    total_models,
    first_year,
    latest_year
FROM manufacturer_stats
ORDER BY make
