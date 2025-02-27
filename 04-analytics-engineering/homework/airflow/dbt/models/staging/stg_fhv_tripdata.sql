{{
    config(
        materialized='view'
    )
}}

SELECT
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    PUlocationID as pickup_location_id,
    DOlocationID as dropoff_location_id
FROM {{ source('staging', 'fhv_tripdata') }}
WHERE dispatching_base_num IS NOT NULL