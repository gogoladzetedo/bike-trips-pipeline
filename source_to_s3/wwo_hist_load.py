from wwo_hist import retrieve_hist_data
from creds import keys
import pandas as pd

def get_weather_df(_start_date, _end_date, _locations_list):

    frequency = 1 # HOURLY
    api_key = keys.wwo_hist_key

    hist_weather_data = retrieve_hist_data(
        api_key, _locations_list, _start_date, _end_date, frequency,location_label = False
        , export_csv = False, store_df = True)

    hist_weather_df = pd.DataFrame(
        hist_weather_data[0])[['location', 'date_time', 'totalSnow_cm', 'FeelsLikeC'
        , 'humidity', 'precipMM', 'tempC', 'visibility', 'windspeedKmph']]
    return hist_weather_df
