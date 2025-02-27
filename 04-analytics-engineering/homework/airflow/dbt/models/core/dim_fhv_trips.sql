{{
    config(
        materialized='table'
    )
}}

SELECT
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    EXTRACT(MONTH FROM pickup_datetime) AS month,
    pickup_location_id,
    dropoff_location_id,
    pickup_zone.zone AS pickup_zone,
    pickup_zone.borough AS pickup_borough,
    dropoff_zone.zone AS dropoff_zone,
    dropoff_zone.borough AS dropoff_borough
FROM {{ ref('stg_fhv_tripdata') }}
LEFT JOIN {{ ref('dim_zones') }} AS pickup_zone
    ON pickup_location_id = pickup_zone.locationid
LEFT JOIN {{ ref('dim_zones') }} AS dropoff_zone
    ON dropoff_location_id = dropoff_zone.locationid