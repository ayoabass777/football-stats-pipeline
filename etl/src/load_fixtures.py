"""
Module to load cleaned fixtures data into the PostgreSQL `raw.raw_fixtures` table using psycopg2 bulk COPY.
- Appends new rows with optional batching
- Avoids truncating or archiving existing content by default
- Streams new data via COPY for high-performance bulk insert
"""
import os
import time
from typing import Optional
import etl.src.config as config
from etl.src.logger import get_logger
import pandas as pd
import psycopg2
from io import StringIO

logger = get_logger(__name__, log_path=config.LOAD_TO_DB_LOG)

# Schema used for raw fixtures
SCHEMA = "raw"

DB_CONFIG = config.DB_CONFIG


def load_to_db(
    parquet_file: str,
    table_name: str = "raw_fixtures",
    if_exists: str = "replace",
    batch_size: Optional[int] = None
) -> None:
    """
    Load cleaned fixtures from a Parquet file into a PostgreSQL table.

    This function:
    - Reads the specified Parquet file into a pandas DataFrame.
    - Connects to Postgres via psycopg2 using DB_CONFIG.
    - Ensures the 'raw' schema exists.
    - Appends or replaces data in raw.<table_name> based on `if_exists`.
    - Uses Postgres COPY to bulk-load the DataFrame, optionally in batches.

    Parameters
    ----------
    parquet_file : str
        Path to the Parquet file to load.
    table_name : str, optional
        Name of the target table under the 'raw' schema (default: "raw_fixtures").
    if_exists : str, optional
        Whether to 'append' (default) or 'replace' existing rows in the target table.
    batch_size : int, optional
        Number of rows per COPY batch. Defaults to loading in a single batch.

    Raises
    ------
    ValueError
        If `parquet_file` is None or `if_exists` has an unsupported value.
    OSError
        If reading or serializing the file fails.
    psycopg2.Error
        If any database operation fails (connection, SQL execution, COPY).
    """
    start_time = time.time()
    if parquet_file is None:
        raise ValueError("parquet_file must be provided")

    if if_exists not in {"append", "replace"}:
        raise ValueError("if_exists must be either 'append' or 'replace'")

    if not os.path.isfile(parquet_file):
        raise FileNotFoundError(f"Parquet file not found: {parquet_file}")

    # 1) Read parquet with timing
    start_parquet = time.time()
    df = pd.read_parquet(parquet_file)
    logger.info(f"Parquet file loaded in {time.time() - start_parquet:.2f}s")

    # 2) Connect via psycopg2 and process within a transaction
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        # 3a) Ensure schema exists
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")

        # 3b) Handle existing table strategy
        cur.execute(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables "
            "  WHERE table_schema = %s AND table_name = %s"
            ");",
            (SCHEMA, table_name)
        )
        exists = cur.fetchone()[0]
        if exists and if_exists == "replace":
            archive_table = f"{SCHEMA}.{table_name}_archive"
            cur.execute(f"DROP TABLE IF EXISTS {archive_table};")
            cur.execute(f"CREATE TABLE {archive_table} AS TABLE {SCHEMA}.{table_name};")
            logger.info(f"Archived raw data to {archive_table}")
            cur.execute(f"TRUNCATE TABLE {SCHEMA}.{table_name};")
            logger.info(f"Truncated table {SCHEMA}.{table_name}")
        elif exists:
            logger.info(f"Appending new rows to existing table {SCHEMA}.{table_name}")
        else:
            logger.info(
                f"Table {SCHEMA}.{table_name} does not exist; ensure it is created before loading."
            )

        total_rows = len(df)
        if total_rows == 0:
            logger.info("No rows to load; skipping COPY execution.")
            return

        # 3c) Bulk load via COPY with timing
        formatted_columns = ', '.join(f'"{col}"' for col in df.columns)
        copy_sql = (
            f"COPY {SCHEMA}.{table_name} ({formatted_columns}) "
            "FROM STDIN WITH CSV"
        )
        if batch_size is None or batch_size <= 0:
            batch_size = total_rows
        num_batches = (total_rows + batch_size - 1) // batch_size
        start_copy = time.time()
        for batch_index, start in enumerate(range(0, total_rows, batch_size), start=1):
            chunk = df.iloc[start:start + batch_size]
            buffer = StringIO()
            chunk.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cur.copy_expert(copy_sql, buffer)
            logger.info(
                f"Batch {batch_index}/{num_batches}: loaded {len(chunk)} rows into {SCHEMA}.{table_name}"
            )
        conn.commit()
        logger.info(f"Loaded {len(df)} rows into {SCHEMA}.{table_name} via COPY in {time.time() - start_copy:.2f}s")
        total_elapsed = time.time() - start_time
        logger.info(f"Total load_to_db duration: {total_elapsed:.2f}s")
    except (OSError, psycopg2.Error) as e:
        logger.error(f"Failed to load data into table '{table_name}': {e}", exc_info=True)
        conn.rollback()
        total_elapsed = time.time() - start_time
        logger.info(f"load_to_db aborted after {total_elapsed:.2f}s due to error")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    try:
        default_file = os.path.join(config.BASE_DATA_DIR, "cleaned_data/cleaned_fixtures_full_20251030_091633.parquet")
        load_to_db(default_file)
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received: shutting down load_to_db gracefully.")
