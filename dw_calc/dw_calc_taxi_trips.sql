-- DC Taxi

DROP TABLE IF EXISTS dc_taxi_trip_details_tmp;
CREATE TEMP TABLE dc_taxi_trip_details_tmp AS
SELECT 
      CAST(TO_TIMESTAMP(T.ORIGINDATETIME_TR, 'MM-DD-YYYY HH24:MI') AS DATE) AS date_id
    , CAST(EXTRACT(HOUR FROM TO_TIMESTAMP(T.ORIGINDATETIME_TR, 'MM-DD-YYYY HH24:MI')) AS INT) AS hour_id
    , S.city_id
    , CAST(NULL AS INT) AS trip_passengers
    , dw.calculate_distance(
          CAST(T.ORIGIN_BLOCK_LATITUDE AS FLOAT) 
        , CAST(T.ORIGIN_BLOCK_LONGITUDE AS FLOAT)
        , CAST(T.DESTINATION_BLOCK_LATITUDE AS FLOAT)
        , CAST(T.DESTINATION_BLOCK_LONGITUDE AS FLOAT)
        , 'K'
    ) AS trip_distance
    , DATE_PART(
              'second'
            , TO_TIMESTAMP(T.ORIGINDATETIME_TR, 'MM-DD-YYYY HH24:MI') - TO_TIMESTAMP(T.ORIGINDATETIME_TR, 'MM-DD-YYYY HH24:MI')
        ) AS trip_duration
    , CAST(T.FAREAMOUNT AS NUMERIC(18, 4)) AS trip_cost_fare
    , CAST(T.GRATUITYAMOUNT AS NUMERIC(18, 4)) trip_cost_tip
    , CAST(T.TOTALAMOUNT AS NUMERIC(18, 4)) As trip_cost_total 

    , CAST(T.ORIGIN_BLOCK_LATITUDE AS FLOAT)  AS pickup_latitude
    , CAST(T.ORIGIN_BLOCK_LONGITUDE AS FLOAT) AS pickup_longitude
    , CAST(T.DESTINATION_BLOCK_LATITUDE AS FLOAT) AS dropoff_latitude
    , CAST(T.DESTINATION_BLOCK_LONGITUDE AS FLOAT) AS dropoff_longitude

    , T.record_id AS staging_record_id
FROM (
    SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY 
        FAREAMOUNT,
        GRATUITYAMOUNT,
        TOTALAMOUNT,
        PAYMENTTYPE,
        MILEAGE,
        DURATION,
        ORIGIN_BLOCK_LATITUDE,
        ORIGIN_BLOCK_LONGITUDE,
        DESTINATION_BLOCK_LATITUDE,
        DESTINATION_BLOCK_LONGITUDE,
        ORIGINDATETIME_TR,
        DESTINATIONDATETIME_TR ORDER BY record_id DESC) AS _Row_Number
    FROM staging.dc_taxi AS T
    WHERE CAST(TO_TIMESTAMP(T.ORIGINDATETIME_TR, 'MM-DD-YYYY HH24:MI') AS DATE) = '{_date}'
) AS T
LEFT JOIN staging.source_systems AS S ON T.source_system = S.NAME
WHERE T._Row_Number = 1 ;


-- NY Taxi
DROP TABLE IF EXISTS ny_taxi_trip_details_tmp;
CREATE TEMP TABLE ny_taxi_trip_details_tmp AS
SELECT 
      CAST(T.pickup_datetime AS DATE) AS date_id
    , CAST(EXTRACT(HOUR FROM CAST(T.pickup_datetime AS TIMESTAMP)) AS INT) AS hour_id
    , S.city_id
    , CAST(
        CASE 
          WHEN T.passenger_count ~ '^\d+\.?\d+$' = TRUE THEN T.passenger_count
          ELSE NULL
        END AS INT
     ) AS trip_passengers
    , CAST(trip_distance AS NUMERIC(18, 4)) * 1.60934 AS trip_distance
    , DATE_PART(
              'second'
            , CAST(T.dropoff_datetime AS TIMESTAMP) - CAST(T.pickup_datetime AS TIMESTAMP)
        ) AS trip_duration
    , CAST(T.fare_amount AS NUMERIC(18, 4)) AS trip_cost_fare
    , CAST(T.tip_amount AS NUMERIC(18, 4)) trip_cost_tip
    , CAST(T.total_amount AS NUMERIC(18, 4)) As trip_cost_total 

    , CAST(pickup_longitude AS FLOAT) AS pickup_longitude
    , CAST(pickup_latitude AS FLOAT) AS pickup_latitude
    , CAST(dropoff_longitude AS FLOAT) AS dropoff_longitude
    , CAST(dropoff_latitude AS FLOAT) AS dropoff_latitude

    , T.record_id AS staging_record_id
FROM (
    SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY 
        pickup_datetime,
        dropoff_datetime,
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type ORDER BY record_id DESC) AS _Row_Number
    FROM staging.ny_taxi AS T
    WHERE CAST(pickup_datetime AS DATE) = '{_date}'
) AS T
LEFT JOIN staging.source_systems AS S ON T.source_system = S.NAME
WHERE T._Row_Number = 1 ;


DROP TABLE IF EXISTS taxi_trip_ids_tmp;
CREATE TEMP TABLE taxi_trip_ids_tmp AS SELECT id FROM dw.fact_taxi_trips WHERE date_id = '{_date}';

DELETE FROM dw.fact_taxi_trips WHERE date_id = '{_date}';

INSERT INTO dw.fact_taxi_trips (
      date_id 
    , hour_id 
    , city_id
    ---
    , trip_passengers
    , trip_distance 
    , trip_duration
    , trip_cost_fare
    , trip_cost_tip
    , trip_cost_total
    ---
    , staging_record_id
)
SELECT
      date_id 
    , hour_id 
    , city_id
    ---
    , trip_passengers
    , trip_distance 
    , trip_duration
    , trip_cost_fare
    , trip_cost_tip
    , trip_cost_total
    ---
    , staging_record_id
FROM dc_taxi_trip_details_tmp
UNION ALL 
SELECT
      date_id 
    , hour_id 
    , city_id
    ---
    , trip_passengers
    , trip_distance 
    , trip_duration
    , trip_cost_fare
    , trip_cost_tip
    , trip_cost_total
    ---
    , staging_record_id
FROM ny_taxi_trip_details_tmp
;

DELETE FROM dw.fact_taxi_trip_locations WHERE taxi_trip_id IN (SELECT id FROM taxi_trip_ids_tmp);

INSERT INTO dw.fact_taxi_trip_locations (
      taxi_trip_id
    , pickup_longitude
    , pickup_latitude
    , dropoff_longitude
    , dropoff_latitude
)
SELECT 
      T.id AS taxi_trip_id
    , TMP.pickup_longitude
    , TMP.pickup_latitude
    , TMP.dropoff_longitude
    , TMP.dropoff_latitude
FROM 
  (SELECT 
      staging_record_id
    , city_id
    , pickup_longitude
    , pickup_latitude
    , dropoff_longitude
    , dropoff_latitude
  FROM dc_taxi_trip_details_tmp
  UNION ALL 
  SELECT 
      staging_record_id
    , city_id
    , pickup_longitude
    , pickup_latitude
    , dropoff_longitude
    , dropoff_latitude
  FROM ny_taxi_trip_details_tmp
  ) AS TMP
    INNER JOIN dw.fact_taxi_trips AS T
    ON TMP.staging_record_id = T.staging_record_id AND T.city_id = TMP.city_id;


DROP TABLE IF EXISTS dc_taxi_trip_details_tmp;
DROP TABLE IF EXISTS ny_taxi_trip_details_tmp;
DROP TABLE taxi_trip_ids_tmp;