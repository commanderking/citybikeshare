import subprocess

# List of Pipfile scripts to run
scripts = [
    "sync_boston",
    "sync_columbus",
    "sync_dc",
    "sync_chicago",
    "sync_nyc",
    "sync_jersey_city",
    "sync_sf",
    "sync_austin",
    "sync_chattanooga",
    "sync_philadelphia",
    "sync_los_angeles",
    "sync_mexico_city",
    "sync_guadalajara",
    "sync_vancouver",  # Vancouver has a 2017 file that won't download properly and is stored as an excel file
    "sync_london",
    "sync_montreal",
]

# Run each script concurrently
processes = [subprocess.Popen(["pipenv", "run", script]) for script in scripts]

# Wait for all processes to complete
for process in processes:
    process.communicate()
