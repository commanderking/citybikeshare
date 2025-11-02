### Purpose

This bikeshare etl pipeline cleans bikeshare data from around the world, and generates easy to analyze parquet folders. It also tries to produce data in a format that is consistent across bikeshare systems, enabling easy comparison of systems.

Currently, data is available for:

| City          | Source |
| -----------   | ----------- |
| Austin        | <https://data.austintexas.gov/Transportation-and-Mobility/Austin-MetroBike-Trips/tyfh-5r8s/about_data> |
| Bergen        | <https://bergenbysykkel.no/en/open-data/historical> |
| Boston        | <https://bluebikes.com/system-data>  |
| Chattanooga   | <https://www.chattadata.org/dataset/Historical-Bike-Chattanooga-Trip-Data/wq49-8xgg/about_data> | 
| Columbus      | <https://cogobikeshare.com/system-data> |
| Chicago       | <https://divvybikes.com/system-data> |
| Guadalajara   | <https://www.mibici.net/es/datos-abiertos/> |
| Jersey City   | <https://citibikenyc.com/system-data> |
| Helsinki      | <https://hri.fi/data/en_GB/dataset/helsingin-ja-espoon-kaupunkipyorilla-ajatut-matkat> |
| London        | <https://cycling.data.tfl.gov.uk/> |
| Los Angeles   | <https://bikeshare.metro.net/about/data/> |
| Mexico City   | <https://ecobici.cdmx.gob.mx/en/open-data/> |
| Montreal      | <https://bixi.com/en/open-data/> |
| NYC           | <https://citibikenyc.com/system-data> |
| Oslo          | <https://oslobysykkel.no/en/open-data/historical> |
| Philadelphia  | <https://www.rideindego.com/about/data/> |
| Pittsburgh    | <https://data.wprdc.org/dataset/pogoh-trip-data> |
| San Francisco | <https://www.lyft.com/bikes/bay-wheels/system-data> |
| Seoul         | <https://data.seoul.go.kr/dataList/OA-15182/F/1/datasetView.do#> |
| Taipei        | <https://data.gov.tw/dataset/150635> | 
| Toronto       | <https://open.toronto.ca/dataset/bike-share-toronto-ridership-data/> |
| Trondheim     | https://trondheimbysykkel.no/en/open-data/historical | 
| Vancouver     | <https://www.mobibikes.ca/en/system-data> | 
| Washington DC | <https://capitalbikeshare.com/system-data> | 

Seoul - data is not processable for a few years because of cleaing challenges on ? and misencodeed characters.

Pittsburgh old data can be found at: https://data.wprdc.org/dataset/healthyride-trip-data


### Prerequisites

1. Install Requirements

- **Python 3.10+**
- **Poetry** 

```
curl -sSL https://install.python-poetry.org | python3 -
```

Then follow instructions to add Poetry to your `PATH`. 

2. Clone the Repo
```
git clone https://github.com/commanderking/citybikeshare.git
cd citybikeshare
```

3. Create a venv (if this is your first time using poetry)

```
poetry config virtualenvs.in-project true
```

4. Install 

```
poetry install
```
### Steps to building a parquet or csv file for a city

```
poetry run citybikeshare pipeline [city]
```

For example:

```
poetry run citybikeshare pipeline boston
```

There are 5 main steps to the process:

1. Sync - downloads the data from the bikeshare website, or syncs it from an amazon s3 bucket 
2. Extract - unzips zip files or handles any other extraction needed from the originally downloaded files
2. Clean - cleans any poorly formatted files, encodes to utf-8 from other languages if needed
3.  Transform - reads the csv data for the city, and creates a corresponding parquet for each file. Then reads all the parquet files and generates an output folder with parquet files partitioned by year and month


Each individual step can be run from the command line as well. For example:

```
poetry run citybikeshare sync oslo
poetry run citybikeshare extract oslo
poetry run citybikeshare clean oslo  
poetry run citybikeshare transform oslo  

```

#### Potential Upcoming Cities in the Pipeline

### Portland
Micromobility contains recent day: https://public.ridereport.com/pdx?x=-122.6543855&y=45.6227107&z=9.70, but individual trip data is unavailable
https://s3.amazonaws.com/biketown-tripdata-public/index.html

### Dublin 
- Only station data at different times

https://data.gov.ie/dataset/dublinbikes-api

### Bicimad
- Stop around February 2023

https://opendata.emtmadrid.es/Datos-estaticos/Datos-generales-(1)

### Hsinchu (?)

https://data.gov.tw/dataset/67784

### Taipei 
- Supposedly contains all trips that were transfers for the month
https://data.gov.tw/dataset/169174

### Seoul

| Seoul         | <https://data.seoul.go.kr/dataList/OA-15182/F/1/datasetView.do#> |

### Daejeon 
https://www.data.go.kr/data/15137219/fileData.do

But when I last tried to download, got an error saying:
"We're sorry, the file you selected is currently in the process of being recovered and cannot be provided at this time."

### Changwon

https://www.data.go.kr/data/15126280/fileData.do?recommendDataYn=Y

Same as Daejeon - button works, and says update, but the download never starts

### Buenos Aires, Argentina

https://data.buenosaires.gob.ar/sk/dataset/bicicletas-publicas 

No duration data, just time of pickup?

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

### Correspondance

- Montreal Open Data says I need to contact Bixi (super fast response)
- Taipei responded within a few days, and the data took a little time to update (two weeks?)
- Chattanooga responded the day of - said data would be available in two weeks
- Spain says there's no plan to returning to publish data. 
- Portland moved to a dashboard, but does not provide granular data any more    