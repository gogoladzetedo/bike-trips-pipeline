DROP TABLE IF EXISTS weather_hist_temp;
CREATE TEMP TABLE weather_hist_temp AS
SELECT 
      CAST(TO_TIMESTAMP(T.date_time, 'YYYY-MM-DD HH24:MI:SS') AS DATE) AS date_id
    , CAST(EXTRACT(HOUR FROM TO_TIMESTAMP(T.date_time, 'YYYY-MM-DD HH24:MI:SS')) AS INT) AS hour_id
    , S.city_id
    , CAST(T.totalSnow_cm AS NUMERIC(18, 4)) AS totalSnow_cm
    , CAST(T.FeelsLikeC AS NUMERIC(18, 4)) AS FeelsLikeC
    , CAST(T.humidity AS NUMERIC(18, 4)) AS humidity
    , CAST(T.precipMM AS NUMERIC(18, 4)) AS precipMM
    , CAST(T.tempC AS NUMERIC(18, 4)) AS tempC
    , CAST(T.visibility AS NUMERIC(18, 4)) AS visibility
    , CAST(T.windspeedKmph AS NUMERIC(18, 4)) AS windspeedKmph
    , T.record_id AS staging_record_id
FROM (
    SELECT T.*, 
        ROW_NUMBER() OVER(PARTITION BY T.location, T.date_time ORDER BY T.record_id DESC) AS _Row_Number
    FROM staging.weather_hist AS T
    WHERE CAST(TO_TIMESTAMP(T.date_time, 'YYYY-MM-DD HH24:MI:SS') AS DATE) = '{_date}'
) AS T
LEFT JOIN staging.source_systems AS S ON T.source_system = S.NAME
WHERE T._Row_Number = 1 ;



DELETE FROM dw.fact_weather WHERE date_id = '{_date}';
INSERT INTO dw.fact_weather (
      date_id 
    , hour_id 
    , city_id 
    -- facts
    , totalSnow_cm 
    , FeelsLikeC 
    , humidity 
    , precipMM
    , tempC
    , visibility
    , windspeedKmph
)
SELECT
      date_id 
    , hour_id 
    , city_id 
    -- facts
    , totalSnow_cm 
    , FeelsLikeC 
    , humidity 
    , precipMM
    , tempC
    , visibility
    , windspeedKmph
FROM weather_hist_temp;

DROP TABLE IF EXISTS weather_hist_temp;





