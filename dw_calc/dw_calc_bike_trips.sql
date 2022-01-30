-- DC Bike
DROP TABLE IF EXISTS dc_bike_trip_details_tmp;
CREATE TEMP TABLE dc_bike_trip_details_tmp AS
SELECT 
      CAST(TO_TIMESTAMP(T.Start_date, 'YYYY-MM-DD HH24:MI') AS DATE) AS date_id
    , CAST(EXTRACT(HOUR FROM TO_TIMESTAMP(T.Start_date, 'YYYY-MM-DD HH24:MI')) AS INT) AS hour_id
    , S.city_id
    , SST.id AS start_station_id
    , EST.id AS end_station_id
    , dw.calculate_distance(
          CAST(SST.latitude AS FLOAT) 
        , CAST(SST.longitude AS FLOAT)
        , CAST(EST.latitude AS FLOAT)
        , CAST(EST.longitude AS FLOAT)
        , 'K'
    ) AS trip_distance
    , CAST(T.Duration AS NUMERIC(18, 4)) AS trip_duration
    , T.record_id AS staging_record_id
FROM (
    SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY 
                    Duration,
                    Start_date,
                    End_date,
                    Start_station_number,
                    Start_station,
                    End_station_number,
                    End_station,
                    Bike_number,
                    Member_type ORDER BY record_id DESC) AS _Row_Number
    FROM staging.dc_bike AS T
    WHERE CAST(TO_TIMESTAMP(T.Start_date, 'YYYY-MM-DD HH24:MI') AS DATE) = '{_date}'
) AS T
LEFT JOIN staging.source_systems AS S ON T.source_system = S.NAME
INNER JOIN dw.dim_bike_station AS SST ON T.Start_station = SST.name
INNER JOIN dw.dim_bike_station AS EST ON T.End_station = EST.name
WHERE T._Row_Number = 1 ;


-- NY bike
DROP TABLE IF EXISTS ny_bike_trip_details_tmp;
CREATE TEMP TABLE ny_bike_trip_details_tmp AS
SELECT 
      CAST(T.starttime AS DATE) AS date_id
    , CAST(EXTRACT(HOUR FROM CAST(T.starttime AS TIMESTAMP)) AS INT) AS hour_id
    , S.city_id
    , SST.id AS start_station_id
    , EST.id AS end_station_id
    , dw.calculate_distance(
          CAST(SST.latitude AS FLOAT) 
        , CAST(SST.longitude AS FLOAT)
        , CAST(EST.latitude AS FLOAT)
        , CAST(EST.longitude AS FLOAT)
        , 'K'
    ) AS trip_distance
    , CAST(tripduration AS NUMERIC(18, 4)) AS trip_duration
    , T.record_id AS staging_record_id
FROM (
    SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY 
                      tripduration
                    , starttime
                    , stoptime
                    , start_station_id
                    , start_station_name
                    , start_station_latitude
                    , start_station_longitude
                    , end_station_id
                    , end_station_name
                    , end_station_latitude
                    , end_station_longitude
                    , bikeid
                    , usertype
                    , birth_year
                    , gender
                    , customer_plan ORDER BY record_id DESC) AS _Row_Number
    FROM staging.ny_bike AS T
    WHERE CAST(starttime AS DATE) = '{_date}'
) AS T
LEFT JOIN staging.source_systems AS S ON T.source_system = S.NAME
INNER JOIN dw.dim_bike_station AS SST ON CAST(T.start_station_id AS INT) = SST.source_system_object_id
INNER JOIN dw.dim_bike_station AS EST ON CAST(T.end_station_id AS INT) = EST.source_system_object_id
WHERE T._Row_Number = 1 ;

--- common

DELETE FROM dw.fact_bike_trips WHERE date_id = '{_date}';
INSERT INTO dw.fact_bike_trips (
      date_id
    , hour_id
    , city_id
    , start_station_id
    , end_station_id
    , trip_distance
    , trip_duration
    , staging_record_id
)
SELECT
      date_id
    , hour_id
    , city_id
    , start_station_id
    , end_station_id
    , trip_distance
    , trip_duration
    , staging_record_id
FROM ny_bike_trip_details_tmp
UNION ALL 
SELECT
      date_id
    , hour_id
    , city_id
    , start_station_id
    , end_station_id
    , trip_distance
    , trip_duration
    , staging_record_id
FROM dc_bike_trip_details_tmp
;


DROP TABLE IF EXISTS dc_bike_trip_details_tmp;
DROP TABLE IF EXISTS ny_bike_trip_details_tmp;