### Purpose

While bikeshare data is often accessible, it requires significant processing before analysis can be done. The repo cleans and merges publicly available bikeshare trip data into a single csv or sqlite db file for further analysis.

Currently, this is only available for Blue Bike data in the greater Boston area.

### Steps to Merge Trip Data into Single File

1. Install pipenv (https://pipenv.pypa.io/en/latest/install/) if needed
2. pipenv install
3. pipenv run sync_boston_bluebike_s3
   - Syncs bluebike data with local file. Files will be stored in ./src/data/bluebikeData
   - Or you can find data here and move relevant files to ./src/data/bluebikeData: https://s3.amazonaws.com/hubway-data/index.html

### Merging all trip data into a single file

To get all bluebike trips (csv, sqlite db, and parquet), run:

`pipenv run generate_blue_bike_trip_data`

To get only one of the formats, add the requested format as an argument

```
pipenv run generate_blue_bike_trip_data --parquet
pipenv run generate_blue_bike_trip_data --csv
pipenv run generate_blue_bike_trip_data --sqlite

```

You'll find the created files in the top level `build` folder.

#### Steps in the script

The script does two things

1. Unzip all bluebike trip data from May, 2018 to now, into their csv files, storing them in `./src/data/monthly_trip_csvs`
2. Reads through the unzipped trip data csvs and merges them into a single file for further analysis

If the data has already been unzipped by running `generate_blue_bike_trip_data`, you can skip the unzipping step by adding `--skip_unzip` to

```
pipenv run generate_blue_bike_trip_data
```

### Notes about the data

Starting in March, 2023, bluebike csvs uploaded to [the s3 bucket](https://s3.amazonaws.com/hubway-data/index.html) changed their format to have different headers than previous csvs. Some of these headers were renamed, such as `starttime` changing to `start_at`, but some old columns were removed and new columns were added.

The following headers are only available prior to March 2023:

```
birth_year
gender
postal_code
usertype
```

Prior to March, 2023, a calculated `trip duration` was also provided. To avoid confusion, old trip durations prior to March, 2023 are not available in the merged data. Trip duration can be calculated by using the `start_time` and `stop_time` headers.

The following headers are only available starting March, 2023:

```
ride_id
rideable_type
member_casual
```

New York Bike shares - new headers start on 02/2021.
