-- Get the latest data from Staging schema
-- dc bike stations
DROP TABLE IF EXISTS dc_bike_stations_temp;
CREATE TEMP TABLE dc_bike_stations_temp AS
SELECT 
      b.OBJECTID
    , b.LATITUDE 
    , b.LONGITUDE 
    , b.NAME
    , b.source_system
FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY OBJECTID ORDER BY record_id DESC) AS _Row_Number
    FROM staging.dc_bike_stations
) AS b
Where b._Row_Number = 1;

-- NY bike data
DROP TABLE IF EXISTS ny_bike_stations_temp;
CREATE TEMP TABLE ny_bike_stations_temp AS
SELECT
      start_station_id
    , start_station_latitude
    , start_station_longitude
    , start_station_name
    , end_station_id
    , end_station_latitude
    , end_station_longitude
    , end_station_name
    , source_system
FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY 
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
    FROM staging.ny_bike
    WHERE CAST(CAST(starttime AS DATE) AS VARCHAR(10)) = '{_date}'
) AS b
WHERE b._Row_Number = 1
GROUP BY 
      start_station_id
    , start_station_latitude
    , start_station_longitude
    , start_station_name
    , end_station_id
    , end_station_latitude
    , end_station_longitude
    , end_station_name
    , source_system ;





-- Create a temp table with staging data and a column to identify new and changed records
-- Data from DC bike stations
DROP TABLE IF EXISTS bike_stations_temp;
CREATE TEMP TABLE bike_stations_temp AS
SELECT 
      ss.id AS source_system_id
    , st.OBJECTID AS source_system_object_id
    , st.LATITUDE AS latitude
    , st.LONGITUDE AS longitude
    , st.NAME AS name
    , CASE 
        WHEN dw.id IS NULL THEN 'Insert'
        WHEN dw.id IS NOT NULL AND NOT
            (
                CAST(st.NAME AS VARCHAR(2000)) = dw.name
            AND CAST(st.LATITUDE AS NUMERIC(9, 6)) = dw.latitude
            AND CAST(st.LONGITUDE AS NUMERIC(9, 6)) = dw.longitude
            ) THEN 'Update'
    END AS Action
FROM dc_bike_stations_temp AS st
INNER JOIN staging.source_systems AS ss
    ON st.source_system = ss.NAME
LEFT JOIN
    dw.dim_bike_station AS dw
    ON ss.id = dw.source_system_id
    AND CAST(st.OBJECTID AS INT) = dw.source_system_object_id
    AND dw.record_end_date IS NULL ;


-- Data from NY bike - start stations
INSERT INTO bike_stations_temp (
      source_system_id, source_system_object_id, latitude, longitude, name, Action)
SELECT 
      ss.id AS source_system_id
    , st.start_station_id AS source_system_object_id
    , st.start_station_latitude AS latitude
    , st.start_station_longitude AS longitude
    , st.start_station_name AS name
    , CASE 
        WHEN dw.id IS NULL THEN 'Insert'
        WHEN dw.id IS NOT NULL AND NOT
            (
                CAST(st.start_station_name AS VARCHAR(2000)) = dw.name
            AND CAST(st.start_station_latitude AS NUMERIC(9, 6)) = dw.latitude
            AND CAST(st.start_station_longitude AS NUMERIC(9, 6)) = dw.longitude
            ) THEN 'Update'
    END AS Action
FROM ny_bike_stations_temp AS st
INNER JOIN staging.source_systems AS ss
    ON st.source_system = ss.NAME
LEFT JOIN
    dw.dim_bike_station AS dw
    ON ss.id = dw.source_system_id
    AND CAST(st.start_station_id AS INT) = dw.source_system_object_id
    AND dw.record_end_date IS NULL;

-- Data from NY bike - end stations
INSERT INTO bike_stations_temp (
      source_system_id, source_system_object_id, latitude, longitude, name, Action)
SELECT 
      ss.id AS source_system_id
    , st.end_station_id AS source_system_object_id
    , st.end_station_latitude AS latitude
    , st.end_station_longitude AS longitude
    , st.end_station_name AS name
    , CASE 
        WHEN dw.id IS NULL THEN 'Insert'
        WHEN dw.id IS NOT NULL AND NOT
            (
                CAST(st.end_station_name AS VARCHAR(2000)) = dw.name
            AND CAST(st.end_station_latitude AS NUMERIC(9, 6)) = dw.latitude
            AND CAST(st.end_station_longitude AS NUMERIC(9, 6)) = dw.longitude
            ) THEN 'Update'
    END AS Action
FROM ny_bike_stations_temp AS st
INNER JOIN staging.source_systems AS ss
    ON st.source_system = ss.NAME
LEFT JOIN
    dw.dim_bike_station AS dw
    ON ss.id = dw.source_system_id
    AND CAST(st.end_station_id AS INT) = dw.source_system_object_id
    AND dw.record_end_date IS NULL;

----------------------------

-- Records that need insert
INSERT INTO dw.dim_bike_station(
      source_system_id
    , source_system_object_id
    , record_start_date
    , record_end_date
    , name
    , longitude
    , latitude)
SELECT 
      CAST(st.source_system_id AS INT)
    , CAST(st.source_system_object_id AS INT)
    , CURRENT_TIMESTAMP
    , NULL
    , st.NAME
    , cast(st.longitude as numeric(9, 6))
    , cast(st.latitude as numeric(9, 6))
From bike_stations_temp As st
Where 
    st.Action = 'Insert';


 -- Records that need update:
 -- 1) close the old records
UPDATE dw.dim_bike_station SET record_end_date =  Current_Timestamp - INTERVAL '1 DAY'
WHERE record_end_date IS NULL
    AND source_system_id IN (
        SELECT CAST(st.source_system_id AS INT) 
        FROM bike_stations_temp AS st
        WHERE action = 'Update'
    )
    AND source_system_object_id IN (
        SELECT CAST(st.source_system_object_id AS INT) 
        FROM bike_stations_temp AS st
        WHERE action = 'Update'
    );
    

 -- 2) add a new record with updated data
INSERT INTO dw.dim_bike_station (
      source_system_id
    , source_system_object_id
    , record_start_date
    , record_end_date
    , name
    , longitude
    , latitude)
SELECT
      cast(st.source_system_id AS INT)
    , cast(st.source_system_object_id AS INT)
    , CURRENT_TIMESTAMP
    , NULL
    , st.NAME
    , cast(st.LONGITUDE AS NUMERIC(9, 6))
    , cast(st.LATITUDE AS NUMERIC(9, 6))
FROM bike_stations_temp AS st
WHERE 
    st.Action = 'Update';


DROP TABLE bike_stations_temp;
DROP TABLE dc_bike_stations_temp;
DROP TABLE ny_bike_stations_temp;