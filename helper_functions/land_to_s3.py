import awswrangler as wr


def landing_path(_city_or_weather, _trip_type, _year, _month):

    if _city_or_weather == 'weather':
        landing_path = f"s3://bike-data-landing-area/landing/{_city_or_weather}/{_year}/{_month}.csv"
    else:
        landing_path = f"s3://bike-data-landing-area/landing/{_city_or_weather}/{_trip_type}/{_year}/{_month}.csv"
    return landing_path

def write_to_s3(_dataframe=None
            , _city_or_weather = 'weather'
            , _trip_type = None
            , _year=None
            , _month=None):
    try:
        wr.s3.to_csv(df=_dataframe, path=landing_path(_city_or_weather, _trip_type, _year, _month),index =False)
    
    except (Exception, wr.exceptions.QueryFailed) as error:
        print(error)
        
    
    
