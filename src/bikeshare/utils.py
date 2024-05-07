import os
import boto3
import pandas as pd
from io import BytesIO
from zipfile import ZipFile

def get_bucket(city):
    if city == "Boston":
        return "hubway-data"

def read_s3_bucket_contents(city, clean_df):
    
    bucket_name = get_bucket(city)
    
    print(bucket_name)
    # Initialize a boto3 S3 client
    s3 = boto3.client('s3')
    
    # List objects within the bucket
    response = s3.list_objects_v2(Bucket=bucket_name)
    
    # DataFrame to hold all the CSV data
    all_data = pd.DataFrame()
    
    # Process each file in the bucket
    for item in response.get('Contents', []):
        key = item['Key']
        
                
        if key.endswith('-tripdata.zip'):
            # Get the object from S3
            obj = s3.get_object(Bucket=bucket_name, Key=key)
            
            # Read the contents of the zip file
            with BytesIO(obj['Body'].read()) as file_stream:
                with ZipFile(file_stream, mode='r') as zip_file:
                    # Extract names of all files within the zip archive
                    for file_name in zip_file.namelist():
                        # Check if the file is a CSV
                        if file_name.endswith('.csv') and '__MACOSX' not in file_name:
                            print(file_name)
                            print('__MACOSX' not in file_name)
                            # Extract the file as a pandas DataFrame
                            with zip_file.open(file_name) as csv_file:
                                df = pd.read_csv(csv_file)
                                clean_df(df)
                                # Append to the main DataFrame
                                all_data = pd.concat([all_data, df], join='outer', ignore_index=True)
                                print(f'total rows {len(all_data.index)}')
    
    return all_data


def get_csv_files(directory):
    trip_files = []
    for file in os.listdir(directory):
        if (file.endswith(".csv")):
            csv_path = os.path.join(directory, file)
            trip_files.append(csv_path)

    return trip_files

def create_file(df, output_path): 
    if output_path.endswith("csv"):
        print ("generating csv...this will take a bit...")
        df.to_csv(output_path, index=True, header=True)
        print("csv file created")
    else: 
        ### https://stackoverflow.com/questions/50604133/convert-csv-to-parquet-file-using-python
        print ("generating parquet... this will take a bit...")
        df.to_parquet(output_path)
        print("parquet file created")