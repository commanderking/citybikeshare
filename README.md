### Purpose

Make it easier for citizens interested in the entire history of bluebike data to play with the data. The code in this repository merges bluebike trip data from publicly avaiable monthly Bluebike trip data into a single csv or sqlite db file.

### Motivation

### Steps to Merge Trip Data into Single File

1. Install pipenv (https://pipenv.pypa.io/en/latest/install/) if needed
2. pipenv install
3. pipenv syncS3
   - Syncs bluebike data with local file. Files will be stored in ./data/bluebikeData
   - Or you can find data here and move relevant files to ./data/bluebikeData: https://s3.amazonaws.com/hubway-data/index.html
