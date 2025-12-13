{{ config(materialized='table') }}

SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY make, model, year) as vehicle_key,
    make,
    model,
    year,
    transmission as vehicle_class,
    displacement,
    cylinders,
    transmission,
    drive as drive_type,
    primary_fuel_type as fuel_type
FROM {{ ref('stg_vehicles') }}
ORDER BY make, model, year
