### Purpose

Citizens interested in public bikeshare data should be able to easily analyze the data to make better decisions for their local community. Current bikeshare data is often difficult to parse through due to the data being in different files and inconsistent data patterns.

The code in this repo cleans and merges publicly available bluebike trip data into a single csv or sqlite db file for analysis.

### Steps to Merge Trip Data into Single File

1. Install pipenv (https://pipenv.pypa.io/en/latest/install/) if needed
2. pipenv install
3. pipenv syncS3
   - Syncs bluebike data with local file. Files will be stored in ./data/bluebikeData
   - Or you can find data here and move relevant files to ./data/bluebikeData: https://s3.amazonaws.com/hubway-data/index.html
