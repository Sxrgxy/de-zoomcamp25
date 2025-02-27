{{
    config(
        materialized='table'
    )
}}

WITH trip_durations AS (
    SELECT
        year,
        month,
        pickup_location_id,
        dropoff_location_id,
        pickup_zone,
        dropoff_zone,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_trips') }}
    WHERE trip_duration > 0 
),

monthly_zone_p90 AS (
    SELECT
        year,
        month,
        pickup_location_id,
        dropoff_location_id,
        pickup_zone,
        dropoff_zone,
        PERCENTILE_CONT(trip_duration, 0.9) OVER (
            PARTITION BY year, month, pickup_location_id, dropoff_location_id
        ) AS p90_duration
    FROM trip_durations
),

ranked_dropoffs AS (
    SELECT DISTINCT
        year,
        month,
        pickup_zone,
        dropoff_zone,
        p90_duration,
        DENSE_RANK() OVER (
            PARTITION BY year, month, pickup_zone 
            ORDER BY p90_duration DESC
        ) AS duration_rank
    FROM monthly_zone_p90
    WHERE year = 2019 AND month = 11
      AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
)

SELECT *
FROM ranked_dropoffs
WHERE duration_rank = 2
ORDER BY pickup_zone