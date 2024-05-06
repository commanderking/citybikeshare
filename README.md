### Purpose

While city bikeshare data is often accessible, it requires significant processing before analysis can be done. The repo cleans and merges publicly available bikeshare trip data into a single csv or parquet file to allow anaylsis on the entire history of bike trips.

Currently, this is only available for bike data in Boston and Washington DC. 

### Steps to Build One File

1. Install pipenv (https://pipenv.pypa.io/en/latest/install/) if needed
2. pipenv install
3. pipenv shell
4. pipenv run build [city] (ex pipenv run build Boston)

You'll find the created files in the top level `build` folder. By default, we generate a parquet file. If you'd like a csv file, you can run 

```
pipenv run build [city] --csv
```

#### Steps in the script

The script does two things

1. Unzip all bikeshare trip data from May, 2018 to now, into their csv files, storing them in `./src/data/[city]_csvs`
2. Merges the unzipped csvs into a single file for further analysis

If the data has already been unzipped by running `pipenv run build`, you can skip the unzipping step by adding `--skip_unzip` to

### Notes about the data


#### Boston
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

#### Washington DC

On May 2020, DC bike data changed their column headers. 

New York Bike shares - new headers start on 02/2021.
