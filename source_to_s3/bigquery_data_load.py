from google.cloud import bigquery
from google.cloud import bigquery_storage

client = bigquery.Client()
bqstorageclient = bigquery_storage.BigQueryReadClient()
config_dry_run = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)


def get_ny_bike(_year, _month):
    sql_query = f"""
        Select tripduration, starttime, stoptime, start_station_id, start_station_name
            , start_station_latitude, start_station_longitude, end_station_id
            , end_station_name, end_station_latitude, end_station_longitude, usertype
        From `bigquery-public-data.new_york_citibike.citibike_trips`
        Where starttime BETWEEN '{_year}-{_month}-01' AND '{_year}-{_month}-31';
        """
  
    ny_bike_df = (
        client.query(sql_query).result().to_dataframe(bqstorage_client=bqstorageclient))
    return ny_bike_df

def get_ny_taxi(_year, _month):
    sql_query = f"""
        Select 
              pickup_datetime, dropoff_datetime, pickup_longitude, pickup_latitude
            , dropoff_longitude, dropoff_latitude, passenger_count, trip_distance
            , fare_amount, tip_amount, total_amount, payment_type
        From `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_{_year}`
        Where pickup_datetime BETWEEN '{_year}-{_month}-01' AND '{_year}-{_month}-31' ;
        """
    
    ny_taxi_df = (
        client.query(sql_query).result().to_dataframe(bqstorage_client=bqstorageclient))
    return ny_taxi_df