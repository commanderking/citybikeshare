## Purpose

This bikeshare ETL pipeline cleans bikeshare data from around the world and produces Parquet datasets that are consistent across systems, so you can compare and analyze them easily.

---

## Setup

**Prerequisites**

- **Python 3.9–3.12**
- **Poetry** (install and add to your `PATH`):

  ```bash
  curl -sSL https://install.python-poetry.org | python3 -
  ```

**Install**

1. Clone the repo and go into the project directory:

   ```bash
   git clone https://github.com/commanderking/citybikeshare.git
   cd citybikeshare
   ```

2. (Optional) Use a virtualenv inside the project:

   ```bash
   poetry config virtualenvs.in-project true
   ```

3. Install dependencies and the CLI:

   ```bash
   poetry install
   ```

4. Run the CLI with:

   ```bash
   poetry run citybikeshare --help
   ```

All commands below assume you run them from the **project root** (`citybikeshare/`).

---

## Getting started

Run the full pipeline for one city (sync → extract → clean → transform):

```bash
poetry run citybikeshare pipeline boston
```

Output lands in:

- **`data/<city>/`** – raw and cleaned data (download, raw, parquet)
- **`output/<city>/`** – final Parquet files partitioned by year/month
- **`analysis/<city>/`** – summary JSON and duration buckets (after you run the analyze commands)

---

## Commands and examples

### Full pipeline

Run all ETL steps for a city. Use `--skip-sync` if you already have raw data.

```bash
poetry run citybikeshare pipeline boston
poetry run citybikeshare pipeline vancouver --skip-sync
```

Pipeline steps: **1. Sync** → **2. Extract** → **3. Clean** → **4. Transform**.

### Individual ETL steps

Run one step at a time (useful when debugging or re-running a single stage).

| Step      | What it does | Example |
| --------- | ------------- | ------- |
| **sync**  | Download or sync raw data (web or S3) | `poetry run citybikeshare sync boston` |
| **extract** | Unzip and extract files into raw CSVs | `poetry run citybikeshare extract boston` |
| **clean** | Normalize encodings, fix formatting | `poetry run citybikeshare clean boston` |
| **transform** | Build Parquet files partitioned by year/month | `poetry run citybikeshare transform boston` |

**Extract** supports overwriting existing extracted files:

```bash
poetry run citybikeshare extract boston --overwrite
```

### Inspect headers

Inspect CSV headers for a city (e.g. to configure or debug):

```bash
poetry run citybikeshare inspect boston
```

### Analysis (after transform)

Generate per-city summary JSON and duration-bucket analysis from the Parquet in `output/<city>/`.

**One city:**

```bash
poetry run citybikeshare analyze boston
```

**All cities** that have output in `output/`:

```bash
poetry run citybikeshare analyze-all
```

Only duration buckets (skip summary):

```bash
poetry run citybikeshare analyze-all --duration_buckets
```

### Merge summaries

Combine per-city summary and duration-bucket files into single JSON files in `analysis/`:

```bash
poetry run citybikeshare merge-summaries
```

Produces `analysis/summary_all_cities.json` and `analysis/duration_buckets_all_cities.json`.

For options on any command: `poetry run citybikeshare <command> --help`.

---

## Supported cities and data sources

Data is available for:

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

Seoul – data is not processable for a few years because of cleaning challenges (encoding and special characters).

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
### Potential upcoming cities

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

### Analysis Decisions

1. Trips where end_time is before the start_time are filtered out.