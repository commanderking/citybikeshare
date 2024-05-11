import os

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