import os
import sys
project_root = os.getenv('PROJECT_ROOT')
sys.path.insert(0, project_root)
import definitions

def get_city_directory(city):
    city_raw_data_path = definitions.DATA_DIR / city
    city_raw_data_path.mkdir(parents=True, exist_ok=True)
    return city_raw_data_path

def get_zip_directory(city):
    path = definitions.DATA_DIR / city / 'zip'
    path.mkdir(parents=True, exist_ok=True)   
    return path

def get_raw_files_directory(city):
    path = definitions.DATA_DIR / city / 'raw'  
    path.mkdir(parents=True, exist_ok=True)   
    return path

def get_output_path(city, file_format):
    path = definitions.DATA_DIR / f'{city}_all_trips.{file_format}'
    return path

def get_csv_files(directory):
    trip_files = []
    for file in os.listdir(directory):
        if (file.endswith(".csv")):
            csv_path = os.path.join(directory, file)
            trip_files.append(csv_path)

    return trip_files

def create_file(df, output_path):
    print(output_path)
    if str(output_path).endswith("csv"):
        print ("generating csv...this will take a bit...")
        df.to_csv(output_path, index=True, header=True)
        print("csv file created")
    else: 
        ### https://stackoverflow.com/questions/50604133/convert-csv-to-parquet-file-using-python
        print ("generating parquet... this will take a bit...")
        df.to_parquet(output_path)
        print(f'parquet file created at {str(output_path)}')