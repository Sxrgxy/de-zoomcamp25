{{
    config(
        materialized='table'
    )
}}

WITH valid_trips AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount
    FROM {{ ref('fact_taxi_trips') }}
    WHERE fare_amount > 0
      AND trip_distance > 0
      AND payment_type_description IN ('Cash', 'Credit Card')
),

monthly_percentiles AS (
    SELECT
        service_type,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97_fare,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95_fare,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90_fare
    FROM valid_trips
)

SELECT DISTINCT
    service_type,
    year,
    month,
    p97_fare,
    p95_fare,
    p90_fare
FROM monthly_percentiles
ORDER BY service_type, year, month