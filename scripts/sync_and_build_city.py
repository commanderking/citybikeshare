import sys
import subprocess
from sync_city import CITY_SYNC_MAP


def sync_and_build_city(city):
    """Sync and build data for a specific city."""
    if city not in CITY_SYNC_MAP:
        print(f"❌ Unknown city: {city}")
        sys.exit(1)

    try:
        # Step 1: Sync data
        print(f"🚀 Syncing {city} data...")
        subprocess.run(f"python3 ./scripts/sync_city.py {city}", shell=True, check=True)

        # Step 2: Build city data
        print(f"🔧 Building {city} data...")
        subprocess.run(f"python3 ./scripts/main.py {city}", shell=True, check=True)

        print(f"✅ Successfully synced and built {city} data!")

    except subprocess.CalledProcessError as e:
        print(f"❌ Error processing {city}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Please specify a city. Example: python sync_and_build.py boston")
        sys.exit(1)

    city_name = sys.argv[1].lower()
    sync_and_build_city(city_name)
