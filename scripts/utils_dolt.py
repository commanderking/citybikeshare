import os
import polars as pl
from sqlalchemy import create_engine, text

HOST = os.getenv("DOLT_DB_HOST")
USER = os.getenv("DOLT_DB_USER")
PASSWORD = os.getenv("DOLT_DB_PASSWORD")
DATABASE = os.getenv("DOLT_DB_DATABASE")


def establish_engine():
    engine = create_engine("mysql+pymysql://root@127.0.0.1:3306/dolt_bikeshare")
    return engine


def is_file_processed(engine, file_metadata):
    """Check if a file has already been processed in DoltDB."""

    name = file_metadata["name"]
    file_hash = file_metadata["file_hash"]

    query = text("SELECT hash FROM processed_trip_files WHERE name = :name")

    with engine.connect() as conn:
        result = conn.execute(query, {"name": name}).fetchone()

    if result:
        stored_hash = result[0]
        return stored_hash == file_hash  # True if file is unchanged, False if changed
    return False  # File has never been processed


def get_system_id(conn, file_metadata):
    system_name = file_metadata["system_name"]
    query = text("SELECT id from bike_systems WHERE system_name = :system_name")
    result = conn.execute(query, {"system_name": system_name}).fetchone()
    return result[0] if result else None


def get_processed_file_id(conn, filename):
    query = text("SELECT id FROM processed_trip_files WHERE name = :filename")
    result = conn.execute(query, {"filename": filename}).fetchone()
    return result[0] if result else None


def insert_trip_data(engine, df, file_metadata):
    name = file_metadata["name"]
    file_hash = file_metadata["file_hash"]
    size = file_metadata["size"]
    modified_at = file_metadata["modified_at"]

    try:
        with engine.connect() as conn:
            try:
                system_id = get_system_id(conn, file_metadata)

                # Insert file metadata into processed_files
                query = text("""
                    INSERT INTO processed_trip_files (name, size, modified_at, hash, system_id) 
                    VALUES (:name, :size, FROM_UNIXTIME(:modified_at), :file_hash, :system_id)
                """)
                conn.execute(
                    query,
                    {
                        "name": name,
                        "size": size,
                        "modified_at": modified_at,
                        "file_hash": file_hash,
                        "system_id": system_id,
                    },
                )

                file_id = get_processed_file_id(conn, name)
                if not file_id:
                    raise Exception(f"Cannot find file_id for file {name}")

                df = df.with_columns(
                    [
                        pl.lit(file_id).alias("processed_file_id"),
                        pl.lit(system_id).alias("system_id"),
                    ]
                )
                collected = df.collect()
                print(collected)
                collected.write_database("trips", conn, if_table_exists="append")

                print(
                    f"✅ Successfully added {name} to trips and processed_trip_files."
                )
                print(
                    "🕵️ Changes ready for manual review with `dolt diff` and `dolt commit`."
                )

            except Exception as e:
                conn.execute(text("CALL DOLT_RESET('--hard')"))
                print(f"❌ Error processing {name}: {e}")
                print("⚠️ Rolled back all Dolt changes via DOLT_RESET.")

    except Exception as e:
        print(f"❌ Fatal DB error: {e}")
