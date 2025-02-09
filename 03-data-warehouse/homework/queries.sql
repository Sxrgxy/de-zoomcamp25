-- create single external table
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp25-448205.ny_taxi.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://terraform-demo-bucket-de-zoomcamp25-448205/raw/yellow/yellow_tripdata_2024-01.parquet',
    'gs://terraform-demo-bucket-de-zoomcamp25-448205/raw/yellow/yellow_tripdata_2024-02.parquet',
    'gs://terraform-demo-bucket-de-zoomcamp25-448205/raw/yellow/yellow_tripdata_2024-03.parquet',
    'gs://terraform-demo-bucket-de-zoomcamp25-448205/raw/yellow/yellow_tripdata_2024-04.parquet',
    'gs://terraform-demo-bucket-de-zoomcamp25-448205/raw/yellow/yellow_tripdata_2024-05.parquet',
    'gs://terraform-demo-bucket-de-zoomcamp25-448205/raw/yellow/yellow_tripdata_2024-06.parquet'
  ]
);

-- create single native table
CREATE OR REPLACE TABLE `de-zoomcamp25-448205.ny_taxi.yellow_taxi_native` AS
SELECT *
FROM `de-zoomcamp25-448205.ny_taxi.yellow_taxi_external`;

-- Question 1: What is count of records for the 2024 Yellow Taxi Data?
-- 20,332,093
SELECT COUNT(*)
FROM `de-zoomcamp25-448205.ny_taxi.yellow_taxi_native`;


-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
--This query will process 155.12 MB when run.
SELECT COUNT(DISTINCT PULocationID)
FROM `de-zoomcamp25-448205.ny_taxi.yellow_taxi_native`;

--This query will process 0 B when run.
SELECT COUNT(DISTINCT PULocationID)
FROM `de-zoomcamp25-448205.ny_taxi.yellow_taxi_external`

-- Question 3: Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery...
--This query will process 155.12 MB when run.
SELECT PULocationID
FROM `de-zoomcamp25-448205.ny_taxi.yellow_taxi_native`;

--This query will process 310.24 MB when run.
SELECT PULocationID, DOLocationID
FROM `de-zoomcamp25-448205.ny_taxi.yellow_taxi_native`;

-- Question 4: How many records have a fare_amount of 0?
-- 8,333
SELECT COUNT(*)
FROM `de-zoomcamp25-448205.ny_taxi.yellow_taxi_native`
WHERE fare_amount = 0;

-- Question 5: What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID
--Partition by tpep_dropoff_datetime and Cluster on VendorID
CREATE OR REPLACE TABLE ny_taxi.yellow_taxi_part_date_clust_vendor
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM ny_taxi.yellow_taxi_external;

--Bytes processed 16.79 MB
--Slot milliseconds 950
SELECT * FROM ny_taxi.yellow_taxi_part_date_clust_vendor
WHERE DATE(tpep_dropoff_datetime) = '2024-05-01'
AND VendorID = 2;

--Cluster on by tpep_dropoff_datetime and Cluster on VendorID
CREATE OR REPLACE TABLE ny_taxi.yellow_taxi_clust_date_vendor
CLUSTER BY tpep_dropoff_datetime, VendorID AS 
SELECT * FROM ny_taxi.yellow_taxi_external;

--Bytes processed 54.86 MB
--Slot milliseconds 3474
SELECT * FROM ny_taxi.yellow_taxi_clust_date_vendor
WHERE DATE(tpep_dropoff_datetime) = '2024-05-01'
AND VendorID = 2;

--Cluster on tpep_dropoff_datetime Partition by VendorID
CREATE OR REPLACE TABLE ny_taxi.yellow_taxi_clust_date_part_vendor
PARTITION BY RANGE_BUCKET(VendorID, GENERATE_ARRAY(1, 3, 1))
CLUSTER BY tpep_dropoff_datetime AS
SELECT * FROM ny_taxi.yellow_taxi_external;

--Bytes processed 45.59 MB
--Slot milliseconds 2496
SELECT * FROM ny_taxi.yellow_taxi_clust_date_part_vendor
WHERE DATE(tpep_dropoff_datetime) = '2024-05-01'
AND VendorID = 2;

--Partition by tpep_dropoff_datetime and Partition by VendorID
-- Error: Only a single PARTITION BY expression is supported but found 2
CREATE OR REPLACE TABLE ny_taxi.yellow_taxi_part_date_part_vendor
PARTITION BY DATE(tpep_dropoff_datetime), RANGE_BUCKET(VendorID, GENERATE_ARRAY(1, 3, 1)) AS
SELECT * FROM ny_taxi.yellow_taxi_external;


-- Question 6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
--This query will process 310.24 MB when run.
SELECT DISTINCT(VendorID)
FROM `de-zoomcamp25-448205.ny_taxi.yellow_taxi_native`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

--This query will process 26.84 MB when run.
SELECT DISTINCT(VendorID)
FROM `de-zoomcamp25-448205.ny_taxi.yellow_taxi_part_date_clust_vendor`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
