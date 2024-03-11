-- Question 0
CREATE MATERIALIZED VIEW latest_dropoff_zones AS SELECT
    taxi_zone.Zone as dropoff_zone,
    max(tpep_dropoff_datetime) AS latest_dropoff_time
FROM
    trip_data
        JOIN taxi_zone
            ON trip_data.DOLocationID = taxi_zone.location_id
GROUP BY
    taxi_zone.Zone
ORDER BY latest_dropoff_time DESC
    LIMIT 10;



-- Question 1 & 2
CREATE MATERIALIZED VIEW summary_stats_zones AS 
WITH t1 AS (SELECT
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        taxi_zone_pu.Zone as pickup_zone,
        taxi_zone_do.Zone as dropoff_zone
    FROM
        trip_data
    JOIN taxi_zone as taxi_zone_pu
        ON trip_data.PULocationID = taxi_zone_pu.location_id
    JOIN taxi_zone as taxi_zone_do
        ON trip_data.DOLocationID = taxi_zone_do.location_id)
SELECT 
    pickup_zone,
    dropoff_zone,
    AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_time,
    MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_time,
    MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_time,
    COUNT(*) AS cnt
FROM t1
GROUP BY pickup_zone, dropoff_zone
ORDER BY avg_time DESC
LIMIT 10;


-- Question 3
CREATE MATERIALIZED VIEW busiest_zones_17_hr AS
SELECT
    taxi_zone.Zone AS pickup_zone,
    count(*) AS last_17_hr_pickup_cnt
FROM
    trip_data
        JOIN taxi_zone
            ON trip_data.PULocationID = taxi_zone.location_id
WHERE
    trip_data.tpep_pickup_datetime > ((SELECT MAX(tpep_pickup_datetime) FROM trip_data) - INTERVAL '17' HOUR)
GROUP BY
    taxi_zone.Zone
ORDER BY last_17_hr_pickup_cnt DESC
LIMIT 10;    
