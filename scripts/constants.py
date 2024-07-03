commonized_system_data_columns = {
    "ride_id": "ride_id",
    "rideable_type": "rideable_type",	
    "started_at": "start_time",	
    "ended_at": "end_time",	
    "start_station_name": "start_station_name",
    "start_station_id": "start_station_id",	
    "end_station_name": "end_station_name",	
    "end_station_id": "end_station_id", 
    "start_lat": "start_station_latitude",	
    "start_lng": "start_station_longitude",
    "end_lat": "end_station_latitude",	
    "end_lng": "end_station_longitude",	
    "member_casual": "member_casual" 
}

boston_renamed_columns_pre_march_2023 = {
    "starttime": "start_time",
    "stoptime": "end_time",
    "start station id": "start_station_id",
    "start station name": "start_station_name",
    "start station latitude": "start_station_latitude",
    "start station longitude": "start_station_longitude",
    "end station id": "end_station_id",
    "end station name": "end_station_name",
    "end station latitude": "end_station_latitude",
    "end station longitude": "end_station_longitude",
    "bikeid": "bike_id",
    "usertype": "usertype",
    "birth year": "birth_year",
    "gender": "gender",
    "postal code": "postal_code"
}

dc_renamed_columns_pre_may_2020 = {
    "Duration": "duration",
    "Start date": "start_time",
    "End date": "end_time",
    "Start station number": "start_station_id",
    "Start station": "start_station_name",
    "End station number": "end_station_id",
    "End station": "end_station_name",
    "Bike number": "bike_number",
    "Member type": "member_type",
}

chicago_renamed_columns_pre_march_2023 = {
    "starttime": "start_time",
    "stoptime": "end_time",
    "from_station_id": "start_station_id",
    "from_station_name": "start_station_name",
    "to_station_id": "end_station_id",
    "to_station_name": "end_station_name",
    "usertype": "usertype",
    "birth year": "birth_year",
    "gender": "gender",
}

# 2018_Q1 2019_Q2 - maybe others
chicago_renamed_columns_oddball = {
    "01 - Rental Details Local Start Time": "start_time",
    "01 - Rental Details Local End Time": "end_time",
    "03 - Rental Start Station ID": "start_station_id",
    "03 - Rental Start Station Name": "start_station_name",
    "02 - Rental End Station ID": "end_station_id",
    "02 - Rental End Station Name": "end_station_name",
    "User Type": "usertype",
    "Member Gender": "gender",
    "05 - Member Details Member Birthday Year": "birth_year"
}

nyc_renamed_columns_initial = {
    "starttime": "start_time",
    "stoptime": "end_time",
    "start station id": "start_station_id",
    "start station name": "start_station_name",
    "start station latitude": "start_station_latitude",
    "start station longitude": "start_station_longitude",
    "end station id": "end_station_id",
    "end station name": "end_station_name",
    "end station latitude": "end_station_latitude",
    "end station longitude": "end_station_longitude",
    "bikeid": "bike_id",
    "usertype": "usertype",
    "birth year": "birth_year",
    "Gender": "gender",
    "postal code": "postal_code"
}

nyc_renamed_columns_2017_03_to_2020_01 = {
    "Trip Duration" : "duration",
    "Start Time": "start_time",
    "Stop Time": "end_time",
    "Start Station ID": "start_station_id",
    "Start Station Name" :"start_station_name",
    "Start Station Latitude" : "start_station_latitude",
    "Start Station Longitude": "start_station_longitude",
    "End Station ID": "end_station_id",
    "End Station Name": "end_station_name",
    "End Station Latitude": "end_station_latitude",
    "End Station Longitude": "end_station_longitude",
    "Bike ID": "bike_id",
    "User Type": "usertype",
    "Birth Year": "birth_year",
    "gender": "gender"
    
}

sf_renamed_columns_pre_may_2020 = {
    "start_time": "start_time",
    "end_time": "end_time",
    "start_station_id": "start_station_id",
    "start_station_name": "start_station_name",
    "start_station_latitude": "start_station_latitude",
    "start_station_longitude": "start_station_longitude",
    "end_station_id": "end_station_id",
    "end_station_name": "end_station_name",
    "end_station_latitude": "end_station_latitude",
    "end_station_longitude": "end_station_longitude",
    "bike_id": "bike_id",
    "user_type": "usertype",
    # In some bike_share_for_all_trip is rental_access_method
    "bike_share_for_all_trip": "birth_year",
}

philadelphia_renamed_columns = {
    "trip_id": "ride_id",
    "duration" : "duration",
    "start_time": "start_time",
    "end_time": "end_time",
    "start_station": "start_station_id",
    "start_lat": "start_station_latiitude",
    "start_lon": "start_station_longitude",
    "end_station": "end_station_id",
    "end_longitude": "end_station_longitude",
    "bike_id": "bike_id",
    "plan_duration": "plan_duration",
    "trip_route_category": "trip_route_category",
    "passholder_type": "passholder_type",
    "bike_type": "bike_type"
}

pittsburgh_renamed_columns = {
    "_id": "id",
    "Closed Status": "closed_status",
    "Duration": "duration",
    "Start Station Id": "start_station_id",
    "Start Date": "start_time",
    "Start Station Name": "start_station_name",
    "End Date": "end_time",
    "End Station Id": "end_station_id",
    "End Station Name": "end_station_name",
    "Rider Type": "rider_type"
}

pittsburgh_healthy_ride_columns = {
    "_id": "id",
    "Trip id": "trip_id",
    "Starttime": "start_time",
    "Stoptime": "end_time",
    "Bikeid": "bike_id",
    "Tripduration": "duration",
    "From station id": "start_station_id",
    "From station name": "start_station_name",
    "To station id": "end_station_id",
    "To station name": "end_station_name",
    "Usertype": "usertype"
}

pittsburgh_healthy_ride_columns_two = {
    "_id": "id",
    "trip_id": "trip_id",
    "starttime": "start_time",
    "stoptime": "end_time",
    "bikeid": "bike_id",
    "tripduration": "duration",
    "from_station_id": "start_station_id",
    "from_station_name": "start_station_name",
    "to_station_id": "end_station_id",
    "to_station_name": "end_station_name",
    "usertype": "usertype"
}


final_columns = ["start_time", "end_time", "start_station_name", "end_station_name"]

philadelphia_final_columns = ["start_time", "end_time", "start_station_id", "end_station_id"]

column_mapping = {
    "boston": [
        {
            "header_matcher": "ride_id",
            "column_mapping": commonized_system_data_columns
        },
        {
            "header_matcher": "bikeid",
            "column_mapping": boston_renamed_columns_pre_march_2023
        }
    ],
    "dc": [
        {
            "header_matcher": "ride_id",
            "column_mapping": commonized_system_data_columns
        },
        {
            "header_matcher": "Bike number",
            "column_mapping": dc_renamed_columns_pre_may_2020
        }
    ],
    "chicago": [
        {
            "header_matcher": "ride_id",
            "column_mapping": commonized_system_data_columns
        },
        {
            "header_matcher": "from_station_name",
            "column_mapping": chicago_renamed_columns_pre_march_2023
        },
        {
            "header_matcher": "01 - Rental Details Local Start Time",
            "column_mapping": chicago_renamed_columns_oddball
        }
    ],
    "nyc": [
        {
            "header_matcher": "ride_id",
            "column_mapping": commonized_system_data_columns
        },
        {
            "header_matcher": "bikeid",
            "column_mapping": nyc_renamed_columns_initial
        },
        {
            "header_matcher": "Trip Duration",
            "column_mapping": nyc_renamed_columns_2017_03_to_2020_01
        }
    ],
    "sf": [
        {
            "header_matcher": "ride_id",
            "column_mapping": commonized_system_data_columns            
        },
        {
            "header_matcher": "bike_id",
            "column_mapping": sf_renamed_columns_pre_may_2020
        }
    ],
    "philadelphia": [
        {
            "header_matcher": "trip_id",
            "column_mapping": philadelphia_renamed_columns,
            "final_columns": philadelphia_final_columns
        }
    ],
    "pittsburgh": [
        {
            "header_matcher": "Start Date",
            "column_mapping": pittsburgh_renamed_columns,
        },
        {
            "header_matcher": "Trip id",
            "column_mapping": pittsburgh_healthy_ride_columns
        },
        {
            "header_matcher": "trip_id",
            "column_mapping": pittsburgh_healthy_ride_columns_two
        }
    ],
    "los_angeles": [
        {
            "header_matcher": "trip_id",
            "column_mapping": philadelphia_renamed_columns,
            "final_columns": philadelphia_final_columns
        },
    ]
}

