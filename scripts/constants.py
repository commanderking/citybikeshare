commonized_system_data_columns = {
    "ride_id": "id",
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
    "member_casual": "member_casual",
}

columbus_columns_one = {
    "trip_id": "id",
    "start_time": "start_time",
    "end_time": "end_time",
    "bikeid": "bike_id",
    "tripduration": "trip_duration",
    "from_station_location": "start_station_location",
    "from_station_id": "start_station_id",
    "from_station_name": "start_station_name",
    "to_station_location": "end_station_location",
    "to_station_id": "end_station_id",
    "to_station_name": "end_station_name",
    "usertype": "usertype",
    "gender": "gender",
    "birthyear": "birth_year",
}

columbus_columns_two = {
    "Start Time and Date": "start_time",
    "Stop Time and Date": "end_time",
    "Start Station ID": "start_station_id",
    "Start Station Name": "start_station_name",
    "Start Station Lat": "start_station_latitude",
    "Start Station Long": "start_station_longitude",
    "Stop Station ID": "end_station_id",
    "Stop Station Name": "end_station_name",
    "Stop Station Lat": "end_station_latitude",
    "Stop Station Long": "end_station_longitude",
    "Bike ID": "bike_id",
    "User Type": "usertype",
    "Gender": "gender",
    "Year of Birth": "birth_year",
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
    "postal code": "postal_code",
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
    "05 - Member Details Member Birthday Year": "birth_year",
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
    "postal code": "postal_code",
}

nyc_renamed_columns_2017_03_to_2020_01 = {
    "Trip Duration": "duration",
    "Start Time": "start_time",
    "Stop Time": "end_time",
    "Start Station ID": "start_station_id",
    "Start Station Name": "start_station_name",
    "Start Station Latitude": "start_station_latitude",
    "Start Station Longitude": "start_station_longitude",
    "End Station ID": "end_station_id",
    "End Station Name": "end_station_name",
    "End Station Latitude": "end_station_latitude",
    "End Station Longitude": "end_station_longitude",
    "Bike ID": "bike_id",
    "User Type": "usertype",
    "Birth Year": "birth_year",
    "gender": "gender",
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

bicycle_transit_systems_renamed_columns = {
    "trip_id": "id",
    "duration": "duration",
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
    "bike_type": "bike_type",
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
    "Rider Type": "rider_type",
}

pittsburgh_healthy_ride_columns = {
    "_id": "id",
    "Trip id": "id",
    "Starttime": "start_time",
    "Stoptime": "end_time",
    "Bikeid": "bike_id",
    "Tripduration": "duration",
    "From station id": "start_station_id",
    "From station name": "start_station_name",
    "To station id": "end_station_id",
    "To station name": "end_station_name",
    "Usertype": "usertype",
}

pittsburgh_healthy_ride_columns_two = {
    "_id": "id",
    "trip_id": "id",
    "starttime": "start_time",
    "stoptime": "end_time",
    "bikeid": "bike_id",
    "tripduration": "duration",
    "from_station_id": "start_station_id",
    "from_station_name": "start_station_name",
    "to_station_id": "end_station_id",
    "to_station_name": "end_station_name",
    "usertype": "usertype",
}

austin_bcycle = {
    "Trip ID": "trip_id",
    "Membership or Pass Type": "pass_type",
    "Bicycle ID": "bicycle_id",
    "Bike Type": "bike_type",
    "Checkout Datetime": "start_time",
    "checkout Date": "start_date",
    "Checkout Kiosk ID": "start_station_id",
    "Checkout Kiosk": "start_station_name",
    "Return Kiosk ID": "end_station_id",
    "Return Kiosk": "end_station_name",
    "Trip Duration Minutes": "duration_minutes",
    "Month": "month",
    "Year": "year",
}

chattanooga_bicycle_transit_system = {
    "Member Type": "member_type",
    "BikeID": "bike_id",
    "Start Time": "start_time",
    "Start Station Name": "start_station_name",
    "Start Station ID": "start_station_id",
    "Start Location": "start_location",
    "End Time": "end_time",
    "End Station Name": "end_station_name",
    "End Station ID": "end_station_id",
    "End Location": "end_location",
    "TripDurationMin": "duration_minutes",
}

final_columns = ["start_time", "end_time", "start_station_name", "end_station_name"]

bicycle_transit_systems_final_columns = [
    "start_time",
    "end_time",
    "start_station_id",
    "end_station_id",
]

norway_renamed_columns = {
    "started_at": "start_time",
    "ended_at": "end_time",
    "start_station_name": "start_station_name",
    "end_station_name": "end_station_name",
}

toronto_initial_columns = {
    "trip_id": "trip_id",
    "trip_start_time": "start_time",
    "trip_stop_time": "end_time",
    "trip_duration_seconds": "duration",
    "from_station_name": "start_station_name",
    "to_station_name": "end_station_name",
    "user_type": "user_type",
}

toronto_renamed_columns = {
    "Trip Id": "trip_id",
    "Trip  Duration": "duration",
    "Start Station Id": "start_station_id",
    "Start Time": "start_time",
    "Start Station Name": "start_station_name",
    "End Station Id": "end_station_id",
    "End Time": "end_time",
    "End Station Name": "end_station_name",
    "Bike Id": "bike_id",
    "User Type": "user_type",
}

vancouver_renamed_columns = {
    "Departure": "start_time",
    "Return": "end_time",
    "Bike": "bike_id",
    "Electric bike": "is_electric_bike",
    "Departure station": "start_station_name",
    "Return station": "end_station_name",
    "Membership type": "membership_type",
    "Covered distance (m)": "covered_distance_meters",
    "Duration (sec.)": "duration_seconds",
    "Stopover duration (sec.)": "stopover_duration",
    "Number of stopovers": "stopover_count",
}


DEFAULT_PROCESSING_PIPELINE = [
    "rename_columns",
    "convert_to_datetime",
    "select_final_columns",
]

config = {
    "columbus": {
        "name": "columbus",
        "system_name": "columbus_bikeshare",
        "file_matcher": "cogo-tripdata",
        "renamed_columns": {
            **commonized_system_data_columns,
            **columbus_columns_one,
            **columbus_columns_two,
        },
        "date_formats": [
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
        ],
        "processing_pipeline": DEFAULT_PROCESSING_PIPELINE,
    },
    "chicago": {
        "name": "chicago",
        "system_name": "chicago_bikeshare",
        "file_matcher": ["trip", "Trips"],
        "renamed_columns": {
            **commonized_system_data_columns,
            **chicago_renamed_columns_pre_march_2023,
            **chicago_renamed_columns_oddball,
        },
        "date_formats": [
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%Y-%m-%d %H:%M",  # Chicago - Divvy_Trips_2013
        ],
        "processing_pipeline": DEFAULT_PROCESSING_PIPELINE,
    },
    "boston": {
        "name": "boston",
        "system_name": "boston_bikeshare",
        "file_matcher": ["-tripdata"],
        "renamed_columns": {
            **commonized_system_data_columns,
            **boston_renamed_columns_pre_march_2023,
        },
        "date_formats": ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"],
    },
    "washington_dc": {
        "name": "washington_dc",
        "system_name": "washington_dc_bikeshare",
        "file_matcher": ["capitalbikeshare-tripdata"],
        "renamed_columns": {
            **commonized_system_data_columns,
            **dc_renamed_columns_pre_may_2020,
        },
        "date_formats": [
            "%Y-%m-%d %H:%M:%S",
        ],
    },
    "new_york_city": {
        "name": "new_york_city",
        "system_name": "new_york_city_bikeshare",
        ### 2022 contains files that have citbike in filename
        "file_matcher": ["citibike-tripdata", "citbike-tripdata"],
        ### NYC files produce two of the same data for 2018 :(
        "excluded_filenames": [
            "2018-citibike-tripdata/201801-citibike-tripdata.csv",
            "2018-citibike-tripdata/201802-citibike-tripdata.csv",
            "2018-citibike-tripdata/201803-citibike-tripdata.csv",
            "2018-citibike-tripdata/201804-citibike-tripdata_1.csv",
            "2018-citibike-tripdata/201804-citibike-tripdata_2.csv",
            "2018-citibike-tripdata/201804-citibike-tripdata.csv",
            "2018-citibike-tripdata/201805-citibike-tripdata.csv",
            "2018-citibike-tripdata/201806-citibike-tripdata.csv",
            "2018-citibike-tripdata/201807-citibike-tripdata.csv",
            "2018-citibike-tripdata/201808-citibike-tripdata.csv",
            "2018-citibike-tripdata/201809-citibike-tripdata.csv",
            "2018-citibike-tripdata/201810-citibike-tripdata.csv",
            "2018-citibike-tripdata/201811-citibike-tripdata.csv",
            "2018-citibike-tripdata/201812-citibike-tripdata.csv"
            ### 2013 - duplicates
            "201306-citibike-tripdata.csv",
            "201307-citibike-tripdata.csv",
            "201308-citibike-tripdata.csv",
            "201309-citibike-tripdata.csv",
            "201310-citibike-tripdata.csv",
            "201311-citibike-tripdata.csv",
            "201312-citibike-tripdata.csv",
        ],
        "renamed_columns": {
            **commonized_system_data_columns,
            **nyc_renamed_columns_initial,
            **nyc_renamed_columns_2017_03_to_2020_01,
        },
        "date_formats": [
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
        ],
    },
    "san_francisco": {
        "name": "san_francisco",
        "system_name": "san_francisco_bikeshare",
        "file_matcher": ["tripdata"],
        "renamed_columns": {
            **commonized_system_data_columns,
            **sf_renamed_columns_pre_may_2020,
        },
        "date_formats": [
            "%Y-%m-%d %H:%M:%S",
        ],
    },
    "philadelphia": {
        "name": "philadelphia",
        "system_name": "philadelphia_bikeshare",
        "file_matcher": ["trips", "Trips"],
        "renamed_columns": {
            **bicycle_transit_systems_renamed_columns,
        },
        "date_formats": [
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M",
        ],
        "processing_pipeline": [
            "rename_columns",
            "process_bicycle_transit_stations",
            "convert_to_datetime",
            "select_final_columns",
            "offset_two_digit_years",
        ],
    },
    "pittsburgh": {
        "name": "pittsburgh",
        "system_name": "pittsburgh_bikeshare",
        "file_matcher": ["csv"],
        "renamed_columns": {
            **pittsburgh_renamed_columns,
            **pittsburgh_healthy_ride_columns,
            **pittsburgh_healthy_ride_columns_two,
        },
        "date_formats": [
            "%m/%d/%Y %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%a, %b %d, %Y, %I:%M %p",  # Pittsburgh one file - 8e8a5cd9-943e-4d21-a7ed-05f865dd0038 (data-id), April 2023,
        ],
    },
    "los_angeles": {
        "name": "los_angeles",
        "system_name": "los_angeles_bikeshare",
        "file_matcher": ["trips"],
        "renamed_columns": {
            **bicycle_transit_systems_renamed_columns,
        },
        "date_formats": [
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M",
        ],
        "processing_pipeline": [
            "rename_columns",
            "process_bicycle_transit_stations",
            "convert_to_datetime",
            "select_final_columns",
        ],
    },
    "austin": {
        "name": "austin",
        "system_name": "austin_bikeshare",
        "file_matcher": ["austin_all_trips.csv"],
        "renamed_columns": {
            **austin_bcycle,
        },
        "date_formats": ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%.6f"],
        "processing_pipeline": [
            "rename_columns",
            "austin_calculate_end_time",
            "convert_to_datetime",
            "select_final_columns",
        ],
    },
    "chattanooga": {
        "name": "chattanooga",
        "system_name": "chattanooga_bikeshare",
        "file_matcher": ["chattanooga_all_trips"],
        "renamed_columns": {
            **chattanooga_bicycle_transit_system,
        },
        "date_formats": ["%m/%d/%Y %I:%M:%S %p"],
    },
    ## Jersey City follows NYC in terms of headers
    "jersey_city": {
        "name": "jersey_city",
        "system_name": "jersey_city_bikeshare",
        "file_matcher": ["JC-"],
        "renamed_columns": {
            **commonized_system_data_columns,
            **nyc_renamed_columns_initial,
            **nyc_renamed_columns_2017_03_to_2020_01,
        },
        "date_formats": [
            "%Y-%m-%d %H:%M:%S",
        ],
    },
    "oslo": {"file_matcher": [".csv"]},
    "trondheim": {"file_matcher": [".csv"]},
    "guadalajara": {"file_matcher": ["datos_abiertos"]},
    "bergen": {
        "name": "bergen",
        "system_name": "bergen_bikeshare",
        "file_matcher": ["trips"],
        "renamed_columns": norway_renamed_columns,
        "date_formats": ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%d %H:%M:%S%:z"],
        "processing_pipeline": DEFAULT_PROCESSING_PIPELINE,
    },
    "toronto": {
        "name": "toronto",
        "system_name": "toronto_bikeshare",
        "file_matcher": [".csv"],
        "renamed_columns": {**toronto_initial_columns, **toronto_renamed_columns},
        "date_formats": ["%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S", "%d/%m/%Y %H:%M"],
        "read_csv_options": {"encoding": "utf8-lossy"},
        "processing_pipeline": [
            "rename_columns",
            "convert_to_datetime",
            "select_final_columns",
            "offset_two_digit_years",
        ],
    },
    "london": {"file_matcher": [".csv"]},
    "montreal": {"file_matcher": ["OD", "DonneesOuvertes"]},
    "vancouver": {
        "file_matcher": ["Mobi_System_Data"],
    },
    "taipei": {"file_matcher": [".csv"]},
}


CONFIG_CITIES = list(config.keys())

GLOBAL_CITIES = [
    "mexico_city",
    "helsinki",
]

ALL_CITIES = CONFIG_CITIES + GLOBAL_CITIES
