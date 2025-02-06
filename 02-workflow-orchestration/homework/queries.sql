--How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
--24,648,499
SELECT COUNT(*)
FROM de-zoomcamp25-448205.ny_taxi.yellow_tripdata
WHERE filename LIKE ("%2020%");

--How many rows are there for the Green Taxi data for all CSV files in the year 2020?
--1,734,051
SELECT COUNT(*)
FROM de-zoomcamp25-448205.ny_taxi.green_tripdata
WHERE filename LIKE ("%2020%");

--How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
--1,925,152
SELECT COUNT(*)
FROM de-zoomcamp25-448205.ny_taxi.yellow_tripdata
WHERE filename = "yellow_tripdata_2021-03.csv.gz";