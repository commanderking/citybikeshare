### Purpose

Citizens should be able to analyze bikeshare data to make better decisions for their local community. While bikeshare data is readily accessible, it often requires further processing before analysis can be done. The code in this repo cleans and merges publicly available bikeshare trip data into a single csv or sqlite db file for further analysis.

Currently, this is only available for bike share data in the greater Boston area.

### Steps to Merge Trip Data into Single File

1. Install pipenv (https://pipenv.pypa.io/en/latest/install/) if needed
2. pipenv install
3. pipenv syncS3
   - Syncs bluebike data with local file. Files will be stored in ./data/bluebikeData
   - Or you can find data here and move relevant files to ./data/bluebikeData: https://s3.amazonaws.com/hubway-data/index.html
