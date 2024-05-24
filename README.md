### Purpose

While city bikeshare data is often accessible, it requires significant processing before analysis can be done. The repo cleans and merges publicly available bikeshare trip data into a single csv or parquet file to allow anaylsis on the entire history of bike trips.

Currently, data is available for:
- Boston
- Chicago
- NYC
- San Francisco
- Taipei
- Washington DC

### Configuration

1. Copy the contents of .env.config to .env. 
2. For PROJECT_ROOT, paste the path to this project. 

### Steps to building a parquet or csv file

1. Install pipenv (https://pipenv.pypa.io/en/latest/install/) if needed
2. pipenv install
3. pipenv shell
4. pipenv run build [city] (ex pipenv run build boston)

By default, a parquet file for your selected city wil lbe generated in the `data` folder. If you'd like a csv file instead, you can run 

```
pipenv run build [city] --csv
```

#### Steps in the script

The general procedure to clean the data in any city is:

1. Unzip all bikeshare trip data into their csv files, storing them in `./src/data/[city]`
2. Commonize the column headers and merge all trips into one polars dataframe.
3. Export a csv or parquet file for further analysis

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

On May 2020, DC bike data changed their column headers in a similar manner.

### Philadelphia

Data can be found here: https://www.rideindego.com/about/data/

### San Francisco

Data can be found here: https://www.lyft.com/bikes/bay-wheels/system-data

### Austin

TODO: Austin updates monthly, but doesn't provide an easy way to download file (need to export) - https://data.austintexas.gov/Transportation-and-Mobility/Austin-MetroBike-Trips/tyfh-5r8s/about_data


### Vancouver

TODO: Vanocuver lists all files, but doesn't have an s3 bucket or API.


### Toronto