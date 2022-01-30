import pandas as pd

def get_dc_taxi(_year, _month):
    filepath = f'data/dc/taxi/taxi_{_year}_{_month}.txt'
    dc_taxi = pd.read_csv(filepath, delimiter='|', header=0, low_memory=False)
    dc_taxi_selected = dc_taxi[[
        'FAREAMOUNT', 'GRATUITYAMOUNT', 'TOTALAMOUNT', 'PAYMENTTYPE', 'MILEAGE'
        , 'DURATION', 'ORIGIN_BLOCK_LATITUDE', 'ORIGIN_BLOCK_LONGITUDE', 'DESTINATION_BLOCK_LATITUDE'
        , 'DESTINATION_BLOCK_LONGITUDE', 'ORIGINDATETIME_TR', 'DESTINATIONDATETIME_TR']]
    return dc_taxi_selected
    
def get_dc_bike(_year, _month):
    quarter_list = [1,1,1,2,2,2,3,3,3,4,4,4]
    curr_quarter = quarter_list[int(_month)-1]

    filepath = f'data/dc/bike/{_year}Q{curr_quarter}-capitalbikeshare-tripdata.csv'
    dc_bike = pd.read_csv(filepath,  delimiter=',', header=0)
    dc_bike['month'] = dc_bike['Start date'].apply(lambda x: x[5:7])
    dc_bike_curr_month = dc_bike[dc_bike['month'] == '01'][
        ["Duration", "Start date", "End date", "Start station number"
        ,"Start station", "End station number", "End station"
        , "Bike number", "Member type"]]
    return dc_bike_curr_month

def get_dc_bike_locations():
    filepath = f'data/dc/bike/locations/Capital_Bike_Share_Locations.csv'
    dc_bike_locations = pd.read_csv(filepath,  delimiter=',', header=0)
    dc_bike_locations_selected = dc_bike_locations[['OBJECTID', 'LATITUDE', 'LONGITUDE', 'NAME']]
    return dc_bike_locations_selected