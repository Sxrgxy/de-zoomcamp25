{{
    config(
        materialized='table'
    )
}}


WITH taxi_trips AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        CONCAT(CAST(EXTRACT(YEAR FROM pickup_datetime) AS STRING), '/Q', 
               CAST(EXTRACT(QUARTER FROM pickup_datetime) AS STRING)) AS year_quarter,
        total_amount
    FROM {{ ref('fact_taxi_trips') }}
),

quarterly_revenue AS (
    SELECT
        service_type,
        year,
        quarter,
        year_quarter,
        SUM(total_amount) AS quarterly_revenue
    FROM taxi_trips
    GROUP BY 1, 2, 3, 4
),

prev_year_revenue AS (
    SELECT
        a.service_type,
        a.year,
        a.quarter,
        a.year_quarter,
        a.quarterly_revenue,
        b.quarterly_revenue AS prev_year_revenue
    FROM quarterly_revenue a
    LEFT JOIN quarterly_revenue b
        ON a.service_type = b.service_type
        AND a.quarter = b.quarter
        AND a.year = b.year + 1
),

yoy_growth AS (
    SELECT
        service_type,
        year,
        quarter,
        year_quarter,
        quarterly_revenue,
        prev_year_revenue,
        CASE 
            WHEN prev_year_revenue IS NULL OR prev_year_revenue = 0 THEN NULL
            ELSE (quarterly_revenue - prev_year_revenue) / prev_year_revenue * 100 
        END AS yoy_growth_pct
    FROM prev_year_revenue
)

SELECT * FROM yoy_growth
ORDER BY service_type, year, quarter