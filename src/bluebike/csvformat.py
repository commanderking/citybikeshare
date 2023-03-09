import os

def get_csv_files(directory):
    ### takes directory where to find csv files and returns as a list
    trip_files = []
    for file in os.listdir(directory):
        if (file.endswith(".csv")):
            csv_path = os.path.join(directory, file)
            trip_files.append(csv_path)

    return trip_files
