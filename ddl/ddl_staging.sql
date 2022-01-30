CREATE SCHEMA IF NOT EXISTS staging; 
CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;

DROP TABLE IF EXISTS staging.source_systems;
CREATE TABLE staging.source_systems (
      id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY
    , NAME VARCHAR(200)
    , city_id INT NULL
);

DROP TABLE IF EXISTS staging.dc_taxi; 
CREATE TABLE IF NOT EXISTS staging.dc_taxi(
record_id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
ingested_at timestamp DEFAULT CURRENT_TIMESTAMP,
source_system VARCHAR(2000) DEFAULT 'dc taxi',

FAREAMOUNT VARCHAR(2000),
GRATUITYAMOUNT VARCHAR(2000),
TOTALAMOUNT VARCHAR(2000),
PAYMENTTYPE VARCHAR(2000),
MILEAGE VARCHAR(2000),
DURATION VARCHAR(2000),
ORIGIN_BLOCK_LATITUDE VARCHAR(2000),
ORIGIN_BLOCK_LONGITUDE VARCHAR(2000),
DESTINATION_BLOCK_LATITUDE VARCHAR(2000),
DESTINATION_BLOCK_LONGITUDE VARCHAR(2000),
ORIGINDATETIME_TR VARCHAR(2000),
DESTINATIONDATETIME_TR VARCHAR(2000)
);


DROP TABLE IF EXISTS staging.ny_taxi; 
CREATE TABLE IF NOT EXISTS staging.ny_taxi(
record_id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
ingested_at timestamp DEFAULT CURRENT_TIMESTAMP,
source_system VARCHAR(2000) DEFAULT 'ny taxi',

pickup_datetime VARCHAR(2000),
dropoff_datetime VARCHAR(2000),
pickup_longitude VARCHAR(2000),
pickup_latitude VARCHAR(2000),
dropoff_longitude VARCHAR(2000),
dropoff_latitude VARCHAR(2000),
passenger_count VARCHAR(2000),
trip_distance VARCHAR(2000),
fare_amount VARCHAR(2000),
tip_amount VARCHAR(2000),
total_amount VARCHAR(2000),
payment_type VARCHAR(2000)
);



DROP TABLE IF EXISTS staging.ny_bike; 
CREATE TABLE IF NOT EXISTS staging.ny_bike(
record_id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
ingested_at timestamp DEFAULT CURRENT_TIMESTAMP,
source_system VARCHAR(2000) DEFAULT 'ny bike',

tripduration VARCHAR(2000),
starttime VARCHAR(2000),
stoptime VARCHAR(2000),
start_station_id VARCHAR(2000),
start_station_name VARCHAR(2000),
start_station_latitude VARCHAR(2000),
start_station_longitude VARCHAR(2000),
end_station_id VARCHAR(2000),
end_station_name VARCHAR(2000),
end_station_latitude VARCHAR(2000),
end_station_longitude VARCHAR(2000),
bikeid VARCHAR(2000),
usertype VARCHAR(2000),
birth_year VARCHAR(2000),
gender VARCHAR(2000),
customer_plan VARCHAR(2000)
);


-- https://www.capitalbikeshare.com/system-data
DROP TABLE IF EXISTS staging.dc_bike; 
CREATE TABLE IF NOT EXISTS staging.dc_bike(
record_id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
ingested_at timestamp DEFAULT CURRENT_TIMESTAMP,
source_system VARCHAR(2000) DEFAULT 'dc bike',

Duration VARCHAR(2000),
Start_date VARCHAR(2000),
End_date VARCHAR(2000),
Start_station_number VARCHAR(2000),
Start_station VARCHAR(2000),
End_station_number VARCHAR(2000),
End_station VARCHAR(2000),
Bike_number VARCHAR(2000),
Member_type VARCHAR(2000)
);


-- https://opendata.dc.gov/datasets/capital-bike-share-locations/explore
DROP TABLE IF EXISTS staging.dc_bike_stations; 
CREATE TABLE IF NOT EXISTS staging.dc_bike_stations(
record_id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
ingested_at timestamp DEFAULT CURRENT_TIMESTAMP,
source_system VARCHAR(2000) DEFAULT 'dc bike',

OBJECTID VARCHAR(2000),
LATITUDE VARCHAR(2000),
LONGITUDE VARCHAR(2000),
NAME VARCHAR(2000)
);


DROP TABLE IF EXISTS staging.weather_hist; 
CREATE TABLE IF NOT EXISTS staging.weather_hist(
record_id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
ingested_at timestamp DEFAULT CURRENT_TIMESTAMP,
source_system VARCHAR(2000) DEFAULT 'wwo',

location VARCHAR(2000),
date_time VARCHAR(2000),
totalSnow_cm VARCHAR(2000),
FeelsLikeC VARCHAR(2000),
humidity VARCHAR(2000),
precipMM VARCHAR(2000),
tempC VARCHAR(2000),
visibility VARCHAR(2000),
windspeedKmph VARCHAR(2000)
);