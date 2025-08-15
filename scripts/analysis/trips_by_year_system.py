import json
import os
import duckdb
import definitions
from pathlib import Path

dolt_path = os.getenv("DOLTPATH")
trips_parquet = Path(dolt_path) / "doltdump" / "trips.parquet"
bike_systems_parquet = Path(dolt_path) / "doltdump" / "bike_systems.parquet"


def output_trip_counts_by_system_and_year():
    print(trips_parquet)

    query = f"""
        SELECT
            trips.system_id,
            bike_systems.system_name AS system_name,
            YEAR(trips.start_time) AS year,
            COUNT(*) AS trip_count,
            MEDIAN(DATE_DIFF('second', trips.start_time, trips.end_time)) AS duration_median,
            QUANTILE_CONT(DATE_DIFF('second', trips.start_time, trips.end_time), 0.25) AS duration_q1,
            QUANTILE_CONT(DATE_DIFF('second', trips.start_time, trips.end_time), 0.75) AS duration_q3
        FROM
            read_parquet('{trips_parquet}') AS trips
        JOIN
            read_parquet('{bike_systems_parquet}') AS bike_systems
        ON trips.system_id = bike_systems.id
        WHERE trips.start_time IS NOT NULL AND trips.end_time IS NOT NULL
        GROUP BY
            trips.system_id,
            bike_systems.system_name,
            YEAR(trips.start_time)
        ORDER BY
            bike_systems.system_name,
            year;
    """

    result = duckdb.query(query).pl().to_dicts()

    with open(
        definitions.ANALYSIS_DIR / "trip_counts_by_system_year.json",
        "w",
    ) as f:
        json.dump(result, f, indent=2)

    print("✅ Exported to trip_counts_by_system_year.json to analysis folder")


def output_partitioned_trips_by_system_and_year():
    # Define output base directory
    trips_dir = Path(definitions.OUTPUT_DIR) / "trips"
    trips_dir.mkdir(parents=True, exist_ok=True)

    # Build query to join and extract trip_year + system_name
    print(
        "🚀 Exporting all trips partitioned by system and year... this will take a while"
    )

    duckdb.sql(f"""
        COPY (
            SELECT 
                trips.*, 
                systems.system_name,
                YEAR(trips.start_time) AS year
            FROM read_parquet('{trips_parquet}') AS trips
            JOIN read_parquet('{bike_systems_parquet}') AS systems
              ON trips.system_id = systems.id
        )
        TO '{trips_dir}' (
            FORMAT PARQUET,
            PARTITION_BY (system_name, year)
        );
    """)

    print(f"✅ Partitioned Parquet export completed to: {trips_dir}")


if __name__ == "__main__":
    output_trip_counts_by_system_and_year()
    output_partitioned_trips_by_system_and_year()
