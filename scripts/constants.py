DEFAULT_FINAL_COLUMNS = [
    "start_time",
    "end_time",
    "start_station_name",
    "end_station_name",
]


DEFAULT_PROCESSING_PIPELINE = [
    "rename_columns",
    "convert_to_datetime",
    "select_final_columns",
]

config = {
    "columbus": {
        "file_matcher": "cogo-tripdata",
    },
    "chicago": {
        "file_matcher": ["trip", "Trips"],
    },
    "boston": {
        "file_matcher": ["-tripdata"],
    },
    "washington_dc": {
        "file_matcher": ["capitalbikeshare-tripdata"],
    },
    "new_york_city": {
        "file_matcher": ["citibike-tripdata", "citbike-tripdata"],
    },
    "san_francisco": {
        "file_matcher": ["tripdata"],
    },
    "philadelphia": {
        "file_matcher": ["trips", "Trips"],
    },
    "pittsburgh": {
        "file_matcher": ["csv"],
    },
    "los_angeles": {
        "file_matcher": ["trips"],
    },
    "austin": {
        "file_matcher": ["austin_all_trips.csv"],
    },
    "chattanooga": {
        "file_matcher": ["chattanooga_all_trips"],
    },
    ## Jersey City follows NYC in terms of headers
    "jersey_city": {
        "file_matcher": ["JC-"],
    },
    "oslo": {"file_matcher": [".csv"]},
    "trondheim": {"file_matcher": [".csv"]},
    "guadalajara": {"file_matcher": ["datos_abiertos"]},
    "toronto": {
        "file_matcher": [".csv"],
    },
    "london": {"file_matcher": [".csv"]},
    "montreal": {"file_matcher": ["OD", "DonneesOuvertes"]},
    "vancouver": {
        "file_matcher": ["Mobi_System_Data"],
    },
    "taipei": {"file_matcher": [".csv"]},
    "helsinki": {"file_matcher": [".zip", ".csv"]},
    "mexico_city": {"file_matcher": [".zip", ".csv"]},
    "bergen": {"file_matcher": ["trips"]},
}


CONFIG_CITIES = list(config.keys())


ALL_CITIES = CONFIG_CITIES
