import os

import helper_functions.land_to_s3 as land


def s3_to_staging_dc_taxi(_landing_path):
    return f"""
        SELECT aws_s3.table_import_from_s3(
            'staging.dc_taxi(
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
                DESTINATIONDATETIME_TR
                )', '', '(FORMAT csv, HEADER true)', 
            aws_commons.create_s3_uri(
                'bike-data-landing-area'
                , '{_landing_path}','eu-north-1'), 
            aws_commons.create_aws_credentials(
                '{os.getenv('AWS_ACCESS_KEY_ID')}', 
                '{os.getenv('AWS_SECRET_ACCESS_KEY')}', '')
        );"""


def s3_to_staging_ny_taxi(_landing_path):
    return f"""
        SELECT aws_s3.table_import_from_s3(
            'staging.ny_taxi(
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
                payment_type
                )', '', '(format csv, header true)', 
            aws_commons.create_s3_uri(
                'bike-data-landing-area'
                , '{_landing_path}','eu-north-1'), 
            aws_commons.create_aws_credentials(
                '{os.getenv('AWS_ACCESS_KEY_ID')}', 
                '{os.getenv('AWS_SECRET_ACCESS_KEY')}', '')
        );"""



def s3_to_staging_dc_bike(_landing_path):
    return f"""
        SELECT aws_s3.table_import_from_s3(
            'staging.dc_bike(
                Duration,
                Start_date,
                End_date,
                Start_station_number,
                Start_station,
                End_station_number,
                End_station,
                Bike_number,
                Member_type
                )', '', '(format csv, header true)', 
            aws_commons.create_s3_uri(
                'bike-data-landing-area'
                , '{_landing_path}','eu-north-1'), 
            aws_commons.create_aws_credentials(
                '{os.getenv('AWS_ACCESS_KEY_ID')}', 
                '{os.getenv('AWS_SECRET_ACCESS_KEY')}', '')
        );"""



def s3_to_staging_dc_bike_stations(_landing_path):
    return f"""
        set client_encoding to 'windows-1251';
        SELECT aws_s3.table_import_from_s3(
            'staging.dc_bike_stations(
                OBJECTID,
                LATITUDE,
                LONGITUDE,
                NAME
                )', '', '(format csv, header true)',  
            aws_commons.create_s3_uri(
                'bike-data-landing-area'
                , '{_landing_path}','eu-north-1'), 
            aws_commons.create_aws_credentials(
                '{os.getenv('AWS_ACCESS_KEY_ID')}', 
                '{os.getenv('AWS_SECRET_ACCESS_KEY')}', '')
        );"""


def s3_to_staging_ny_bike(_landing_path):
    return f"""
        SELECT aws_s3.table_import_from_s3(
            'staging.ny_bike(
                tripduration,
                starttime,
                stoptime,
                start_station_id,
                start_station_name,
                start_station_latitude,
                start_station_longitude,
                end_station_id,
                end_station_name,
                end_station_latitude,
                end_station_longitude,
                bikeid,
                usertype,
                birth_year,
                gender,
                customer_plan
                )', '', '(format csv, header true)', 
            aws_commons.create_s3_uri(
                'bike-data-landing-area'
                , '{_landing_path}','eu-north-1'), 
            aws_commons.create_aws_credentials(
                '{os.getenv('AWS_ACCESS_KEY_ID')}', 
                '{os.getenv('AWS_SECRET_ACCESS_KEY')}', '')
        );"""


def s3_to_staging_weather(_landing_path):
    return f"""
        SELECT aws_s3.table_import_from_s3(
            'staging.weather_hist(
                location,
                date_time,
                totalSnow_cm,
                FeelsLikeC,
                humidity,
                precipMM,
                tempC,
                visibility,
                windspeedKmph
                )', '', '(format csv, header true)',  
            aws_commons.create_s3_uri(
                'bike-data-landing-area'
                , '{_landing_path}','eu-north-1'), 
            aws_commons.create_aws_credentials(
                '{os.getenv('AWS_ACCESS_KEY_ID')}', 
                '{os.getenv('AWS_SECRET_ACCESS_KEY')}', '')
        );"""
