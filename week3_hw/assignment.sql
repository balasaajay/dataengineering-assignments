
-- Q1
SELECT COUNT(*) FROM `de-zoomcamp-ga.fhv_data.2019_native`;

-- Q3
SELECT COUNT(*) FROM `de-zoomcamp-ga.fhv_data.2019_native` WHERE PUlocationID is null AND  	
DOlocationID is null;

-- Q5 
-- Create partioned table
CREATE OR REPLACE TABLE `de-zoomcamp-ga.fhv_data.2019_partitioned` PARTITION BY 
DATE(pickup_datetime) AS
SELECT * FROM de-zoomcamp-ga.fhv_data.2019;

-- Q5
-- Creating a cluster from partitions 
CREATE OR REPLACE TABLE de-zoomcamp-ga.fhv_data.2019_partitioned_clustered
PARTITION BY DATE(pickup_datetime) 
CLUSTER BY Affiliated_base_number AS
SELECT * FROM de-zoomcamp-ga.fhv_data.2019;

-- Q5 
SELECT DISTINCT(Affiliated_base_number)
FROM de-zoomcamp-ga.fhv_data.2019_partitioned
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
