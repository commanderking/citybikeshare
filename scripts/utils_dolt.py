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
        with engine.begin() as conn:
            system_id = get_system_id(conn, file_metadata)

            ### Check if same file name already exists and compare file size
            existing = conn.execute(
                text("""
                    SELECT id, size
                    FROM processed_trip_files
                    WHERE name = :name AND system_id = :system_id
                    LIMIT 1
                """),
                {"name": name, "system_id": system_id},
            ).fetchone()

            if existing:
                existing_id, existing_size = existing
                if existing_size == size:
                    print(f"Skipping {name}: same size ({size}) already recorded.")
                    return

                # Delete from processed_trip_files as we need to reinsert
                conn.execute(
                    text("DELETE FROM processed_trip_files WHERE id = :id"),
                    {"id": existing_id},
                )
                print(f"Replacing {name}: size changed {existing_size} → {size}.")

            ### Insert new file metadata
            conn.execute(
                text("""
                    INSERT INTO processed_trip_files (name, size, modified_at, hash, system_id)
                    VALUES (:name, :size, FROM_UNIXTIME(:modified_at), :file_hash, :system_id)
                """),
                {
                    "name": name,
                    "size": size,
                    "modified_at": modified_at,
                    "file_hash": file_hash,
                    "system_id": system_id,
                },
            )

            ### Pull the nely added file metadata from the database
            file_id = conn.execute(
                text("""
                    SELECT id
                    FROM processed_trip_files
                    WHERE name = :name AND system_id = :system_id
                    ORDER BY modified_at DESC, id DESC
                    LIMIT 1
                """),
                {"name": name, "system_id": system_id},
            ).scalar()

            if not file_id:
                raise Exception(f"Cannot find file_id for file {name}")

            ### Add foreign key/system columns and write trips
            df_to_write = df.with_columns(
                [
                    pl.lit(file_id).alias("processed_file_id"),
                    pl.lit(system_id).alias("system_id"),
                ]
            ).collect()

            df_to_write.write_database("trips", conn, if_table_exists="append")

        # Transaction auto-commits on success
        print(f"✅ Successfully added {name} to trips and processed_trip_files.")
        return "inserted_or_replaced"

    except Exception as e:
        ### Reset dolt database on failure
        try:
            with engine.connect() as c:
                c.execute(text("CALL DOLT_RESET('--hard')"))
        except Exception as reset_err:
            print(f"⚠️ DOLT_RESET failed: {reset_err}")
        print(f"❌ Error processing {name}: {e}")
        print("⚠️ Rolled back all Dolt changes via DOLT_RESET.")
        return "error"
