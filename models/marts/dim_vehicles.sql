{{ config(materialized='table') }}

SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY make, model, year) as vehicle_key,
    make,
    model,
    year,
    VClass as vehicle_class,
    displ as displacement,
    cylinders,
    trany as transmission,
    drive as drive_type,
    fuelType as fuel_type
FROM {{ ref('stg_vehicles') }}
ORDER BY make, model, year
