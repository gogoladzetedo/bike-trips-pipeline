
#import local libraries
import source_to_s3.wwo_hist_load as weather
import helper_functions.land_to_s3 as land
import source_to_s3.local_files_load as locf
import source_to_s3.bigquery_data_load as bqdata
import db.rds_connect as dbcon
import s3_to_stg.s3_to_staging as s3_stg
import awswrangler as wr
import boto3
import logging
logging.basicConfig(level=logging.INFO)

# Create staging schema and objects

def run_ddl():
    logging.info('Creating staging tables')
    input_ddl_file_staging = open("ddl/ddl_staging.sql").read()
    dbcon.rds_exec_gracefully(input_ddl_file_staging)

    logging.info('Creating dw tables')
    input_ddl_file_dw = open("ddl/ddl_dw.sql").read()
    dbcon.rds_exec_gracefully(input_ddl_file_dw)

    logging.info('Inserting source systems data')
    input_file_stagin_sources = open("ddl/staging_source_systems.sql").read()
    dbcon.rds_exec_gracefully(input_file_stagin_sources)


s3 = boto3.client('s3')


def run_source_to_s3():
    logging.info('source -> s3: weather')
    land.write_to_s3(weather.get_weather_df('01-JAN-2017', '03-JAN-2017', ['new-york', 'washington-dc'])
                    , _year='2017', _month = '01')
    
    logging.info('source -> s3: dc bike')
    land.write_to_s3(locf.get_dc_bike('2017', '01') , 'dc', 'bike', '2017', '01')

    logging.info('source -> s3: dc_taxi')
    land.write_to_s3(locf.get_dc_taxi('2017', '01') , 'dc', 'taxi', '2017', '01')

    logging.info('source -> s3: ny_bike')
    land.write_to_s3(bqdata.get_ny_bike('2017', '01') , 'ny', 'bike', '2017', '01')

    logging.info('source -> s3: ny taxi')
    land.write_to_s3(bqdata.get_ny_taxi('2017', '01') , 'ny', 'taxi', '2017', '01')

    logging.info('source -> s3: dc bike stations')
    wr.s3.to_csv(df=locf.get_dc_bike_locations()
        , path='s3://bike-data-landing-area/landing/dc/bike/locations/locations.csv',index =False)


def run_s3_to_rds_staging():
    logging.info('start of s3->RDS: dc_taxi')
    dc_taxi_full_path= land.landing_path('dc', 'taxi', '2017', '01')
    dc_taxi_full_path_wo_bucket = dc_taxi_full_path[28: len(dc_taxi_full_path)]
    to_staging_dc_taxi = s3_stg.s3_to_staging_dc_taxi(dc_taxi_full_path_wo_bucket)
    dbcon.rds_exec_gracefully(to_staging_dc_taxi)

    logging.info('start of s3->RDS: ny_taxi')
    ny_taxi_full_path= land.landing_path('ny', 'taxi', '2017', '01')
    dc_taxi_full_path_wo_bucket = ny_taxi_full_path[28: len(ny_taxi_full_path)]
    to_staging_ny_taxi = s3_stg.s3_to_staging_ny_taxi(dc_taxi_full_path_wo_bucket)
    dbcon.rds_exec_gracefully(to_staging_ny_taxi)

    logging.info('start of s3->RDS: ny_bike')
    ny_bike_full_path= land.landing_path('ny', 'bike', '2017', '01')
    ny_bike_full_path_wo_bucket = ny_bike_full_path[28: len(ny_bike_full_path)]
    to_staging_ny_bike = s3_stg.s3_to_staging_ny_bike(ny_bike_full_path_wo_bucket)
    dbcon.rds_exec_gracefully(to_staging_ny_bike)

    logging.info('start of s3->RDS: dc_bike')
    dc_bike_full_path= land.landing_path('dc', 'bike', '2017', '01')
    dc_bike_full_path_wo_bucket = dc_bike_full_path[28: len(dc_bike_full_path)]
    to_staging_dc_bike = s3_stg.s3_to_staging_dc_bike(dc_bike_full_path_wo_bucket)
    dbcon.rds_exec_gracefully(to_staging_dc_bike)

    logging.info('start of s3->RDS: dc_bike_locations')
    dc_bike_stations_full_path_wo_bucket = 'landing/dc/bike/locations/locations.csv'
    to_staging_dc_bike_stations = s3_stg.s3_to_staging_dc_bike_stations(dc_bike_stations_full_path_wo_bucket)
    dbcon.rds_exec_gracefully(to_staging_dc_bike_stations)

    logging.info('start of s3->RDS: weather')
    weather_full_path= land.landing_path('weather', '', '2017', '01')
    weather_full_path_wo_bucket = weather_full_path[28: len(weather_full_path)]
    to_staging_weather = s3_stg.s3_to_staging_weather(weather_full_path_wo_bucket)
    dbcon.rds_exec_gracefully(to_staging_weather)
    
# DW calculations
def run_staging_to_dw():
    logging.info('Staging->DW: Taxi trips')
    input_file_dw_taxi_trips = open("dw_calc/dw_calc_taxi_trips.sql").read()
    dbcon.rds_exec_gracefully(input_file_dw_taxi_trips.format(_date = '2017-01-01'))

    logging.info('Staging->DW: Bike stations')
    input_file_dw_bike_stations = open("dw_calc/dw_calc_dim_bike_stations.sql").read()
    dbcon.rds_exec_gracefully(input_file_dw_bike_stations.format(_date = '2017-01-01'))

    logging.info('Staging->DW: Bike trips')
    input_file_dw_bike_trips = open("dw_calc/dw_calc_bike_trips.sql").read()
    dbcon.rds_exec_gracefully(input_file_dw_bike_trips.format(_date = '2017-01-01'))

    logging.info('Staging->DW: Weather')
    input_file_dw_weather = open("dw_calc/dw_calc_weather.sql").read()
    dbcon.rds_exec_gracefully(input_file_dw_weather.format(_date = '2017-01-01'))


logging.info('start of: checking the data')
df_rowcounts = dbcon.rds_select_df(""" 
    SELECT 'ny_taxi', Count(*) AS _Rows FROM staging.ny_taxi
    UNION ALL 
    SELECT 'dc_taxi', Count(*) AS _Rows FROM staging.dc_taxi
    UNION ALL
    SELECT 'ny_bike', Count(*) AS _Rows FROM staging.ny_bike
    UNION ALL
    SELECT 'dc_bike', Count(*) AS _Rows FROM staging.dc_bike
    UNION ALL
    SELECT 'dc_bike_stations', Count(*) AS _Rows FROM staging.dc_bike_stations
    UNION ALL
    SELECT 'weather_hist', Count(*) AS _Rows FROM staging.weather_hist
    UNION ALL
    SELECT 'dim_bike_station', Count(*) AS _Rows FROM dw.dim_bike_station
    UNION ALL
    SELECT 'fact_taxi_trip_locations', Count(*) AS _Rows FROM dw.fact_taxi_trip_locations
    UNION ALL
    SELECT 'fact_taxi_trips', Count(*) AS _Rows FROM dw.fact_taxi_trips
    UNION ALL
    SELECT 'fact_bike_trips', Count(*) AS _Rows FROM dw.fact_bike_trips
    UNION ALL
    SELECT 'fact_weather', Count(*) AS _Rows FROM dw.fact_weather
""")

#stg_bs = dbcon.rds_select_df("Select * From dw.fact_weather")


run_ddl()
run_source_to_s3()
run_s3_to_rds_staging()
run_staging_to_dw()

print(df_rowcounts)

#print(stg_bs)
