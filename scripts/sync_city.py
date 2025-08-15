import sys
import subprocess

CITY_SYNC_MAP = {
    # aws sync
    "boston": "aws s3 sync s3://hubway-data ./data/boston/zip",
    "columbus": "aws s3 sync s3://cogo-sys-data ./data/columbus/zip",
    "washington_dc": "aws s3 sync s3://capitalbikeshare-data ./data/washington_dc/zip",
    "chicago": "aws s3 sync s3://divvy-tripdata ./data/chicago/zip",
    "new_york_city": "aws s3 sync s3://tripdata ./data/new_york_city/zip",
    "london": "aws s3 sync s3://cycling.data.tfl.gov.uk/usage-stats ./data/london/raw",
    "jersey_city": "aws s3 sync s3://tripdata ./data/jersey_city/zip --exclude '*' --include 'JC-*'",
    "san_francisco": "aws s3 sync s3://baywheels-data ./data/san_francisco/zip",
    # Cities with custom scripts
    "guadalajara": "python3 ./scripts/city/guadalajara.py",
    "austin": "python3 ./scripts/city/austin.py",
    "bergen": "python3 ./scripts/city/bergen.py",
    "chattanooga": "python3 ./scripts/city/chattanooga.py",
    "mexico_city": "python3 ./scripts/city/mexico_city.py",
    "montreal": "python3 ./scripts/city/montreal.py",
    "philadelphia": "python3 ./scripts/city/philadelphia.py",
    "pittsburgh": "python3 ./scripts/city/pittsburgh.py",
    "los_angeles": "python3 ./scripts/city/los_angeles.py",
    "oslo": "python3 ./scripts/city/oslo.py",
    "trondheim": "python3 ./scripts/city/trondheim.py",
    "vancouver": "python3 ./scripts/city/vancouver.py",
    "toronto": "python3 ./scripts/city/toronto.py",
}


def sync_city(city):
    """Sync a city's bikeshare data."""
    if city not in CITY_SYNC_MAP:
        print(f"❌ Unknown city: {city} - add city to CITY_SYNC_MAP ")
        sys.exit(1)

    command = CITY_SYNC_MAP[city]
    print(f"🚀 Running sync for {city}: {command}")
    subprocess.run(command, shell=True, check=True)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Please specify a city. Example: python sync_city.py boston")
        sys.exit(1)

    city_name = sys.argv[1].lower()
    sync_city(city_name)
