### Purpose

While city bikeshare data is often accessible, it requires significant processing before analysis can be done. The repo cleans and merges publicly available bikeshare trip data into a single csv or parquet file to allow anaylsis on the entire history of bike trips.

Currently, data is available for:

| City          | Source |
| -----------   | ----------- |
| Austin        | <https://data.austintexas.gov/Transportation-and-Mobility/Austin-MetroBike-Trips/tyfh-5r8s/about_data> |
| Bergen        | <https://bergenbysykkel.no/en/open-data/historical> |
| Boston        | <https://bluebikes.com/system-data>  |
| Chattanooga   | <https://internal.chattadata.org/Recreation/Bike-Chattanooga-Trip-Data/tdrg-39c4/about_data> | 
| Columbus      | <https://cogobikeshare.com/system-data> |
| Chicago       | <https://divvybikes.com/system-data> |
| Jersey City   | <https://citibikenyc.com/system-data> |
| Los Angeles   | <https://bikeshare.metro.net/about/data/> |
| Mexico City   | <https://ecobici.cdmx.gob.mx/en/open-data/> |
| Montreal      | <https://bixi.com/en/open-data/> |
| NYC           | <https://citibikenyc.com/system-data> |
| Oslo          | <https://oslobysykkel.no/en/open-data/historical> |
| Philadelphia  | <https://www.rideindego.com/about/data/> |
| Pittsburgh    | <https://data.wprdc.org/dataset/pogoh-trip-data> |
| San Francisco | <https://www.lyft.com/bikes/bay-wheels/system-data> |
| Taipei        | <https://data.gov.tw/dataset/150635> | 
| Toronto       | <https://open.toronto.ca/dataset/bike-share-toronto-ridership-data/> |
| Trondheim     | https://trondheimbysykkel.no/en/open-data/historical | 
| Vancouver     | <https://www.mobibikes.ca/en/system-data> | 
| Washington DC | <https://capitalbikeshare.com/system-data> | 

Pittsburgh old data can be found at: https://data.wprdc.org/dataset/healthyride-trip-data

### Configuration

1. Copy the contents of .env.config to .env. 
2. For PROJECT_ROOT, paste the path to this project on your local machine

### Steps to building a parquet or csv file

1. Install pipenv (https://pipenv.pypa.io/en/latest/install/) if needed
2. pipenv install
3. pipenv shell
4. pipenv run build [city] (ex pipenv run build boston)

By default, a parquet file for your selected city wil lbe generated in the `data` folder. If you'd like a csv file instead, you can run 

```
pipenv run build [city] --csv
```

#### Steps in creating a parquet file for a city's bikeshare data

The general procedure to clean the data in any city is:

1. Unzip all bikeshare trip data into their csv files, storing them in `./src/data/[city]`
2. Commonize the column headers and merge all trips into one polars dataframe.
3. Export a csv or parquet file for further analysis

If the data has already been unzipped by running `pipenv run build`, you can skip the unzipping step by adding `--skip_unzip` to

#### Potential Upcoming Cities in the Pipeline

### London 
https://cycling.data.tfl.gov.uk/

### Helsinki

https://hri.fi/data/en_GB/dataset/helsingin-ja-espoon-kaupunkipyorilla-ajatut-matkat

https://github.com/Geometrein/helsinki-city-bikes/blob/main/scraper.py

### Portland
Micromobility contains recent day: https://public.ridereport.com/pdx?x=-122.6543855&y=45.6227107&z=9.70, but individual trip data is unavailable
https://s3.amazonaws.com/biketown-tripdata-public/index.html

### Dublin 
- Only station data at different times

https://data.gov.ie/dataset/dublinbikes-api

### Bicimad
- Stop around February 2023

https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales-(1)

### Hsinchu 

https://data.gov.tw/dataset/67784

### Taipei 
- Supposedly contains all trips that were transfers for the month
https://data.gov.tw/dataset/169174


#### Null start and end stations - tested on 6/12/24. Includes 2024/04 bluebike data
- Chicago - 4_102_485
- Boston - 19_626
- NYC - 308_238
- Philadelphia - 0
- DC - 1_563_414
- SF - 3_205_547
- Toronto - 16 
- Taipei - 692

### Data Cleaning Challenges

- Pittsburgh has one file that isn't accessible through the API

- Philadelphia 2015 is in /15 format for year

- NYC has duplicate set of data for 2018 that needs proper filtering

- Chicago bikeshare study - https://www.mdpi.com/2071-1050/16/5/2146
https://www.sciencedirect.com/science/article/abs/pii/S0965856420306479

Mexico City Issues 
-  ecobici_2022_12.csv has two misnamed headers Ciclo_EstacionArribo, Fech Arribo (no underscore)
- 2010-10.csv has 3 null values


### Resources 

- https://bikeshare-research.org/#/

## Tentative Ranking Rubric

### Data Consistency
- Consistent column headers naming throughout the years 

### Data Variety 
- How much data do they expose? Stop times? Membership type? 

### Unique areas 
- Do they do things that others don't? 
- Make ride times anonymous (Taipei and Vancouver) 



### Correspondance

- Montreal Open Data says I need to contact Bixi (super fast response)
- Taipei responded within a few days, and the data took a little time to update (two weeks?)
- Chattanooga responded the day of - said data would be available in two weeks
- Spain says there's no plan to returning to publish data. 
- Portland moved to a dashboard, but does not provide granular data any more