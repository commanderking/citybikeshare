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

## TODO: Update to dynamically generate results
ALL_CITIES = [
    "columbus",
    "chicago",
    "boston",
    "washington_dc",
    "new_york_city",
    "san_francisco",
    "philadelphia",
    "pittsburgh",
    "los_angeles",
    "austin",
    "chattanooga",
    "jersey_city",
    "oslo",
    "trondheim",
    "guadalajara",
    "toronto",
    "london",
    "montreal",
    "vancouver",
    "taipei",
    "helsinki",
    "mexico_city",
    "bergen",
]
