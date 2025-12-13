{{ config(materialized='table') }}

SELECT DISTINCT
    period as date_key,
    period as full_date,
    EXTRACT(YEAR FROM period) as year,
    EXTRACT(MONTH FROM period) as month,
    EXTRACT(QUARTER FROM period) as quarter,
    EXTRACT(WEEK FROM period) as week,
    FORMAT_DATE('%B', period) as month_name,
    FORMAT_DATE('%A', period) as day_name
FROM {{ ref('stg_fuel_prices') }}
ORDER BY period
