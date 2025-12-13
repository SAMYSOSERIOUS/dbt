{{ config(materialized='table') }}

SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY make) as manufacturer_key,
    make as manufacturer_name,
    COUNT(*) OVER (PARTITION BY make) as total_models,
    MIN(year) OVER (PARTITION BY make) as first_year,
    MAX(year) OVER (PARTITION BY make) as latest_year
FROM {{ ref('stg_vehicles') }}
ORDER BY make
