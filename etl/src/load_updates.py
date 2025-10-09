"""
Module to apply processed fixture updates by merging transformed data back into the database.
Reads the list of updated fixture IDs from a Parquet file and the latest
cleaned Parquet to update all relevant columns in raw.raw_fixtures.
"""

import os
import time
from typing import Any, Optional
from io import StringIO

import pandas as pd
import psycopg2
from pathlib import Path
from psycopg2 import sql
import argparse

from etl.src.config import CLEANED_DATA_DIR, LOAD_UPDATES_LOG, FIXTURES_UPDATE_DIR
from etl.src.extract_metadata import get_db_connection
from etl.src.transform_fixtures import write_outputs
from etl.src.logger import get_logger

logger = get_logger(__name__, log_path=LOAD_UPDATES_LOG)

# Column used to join updates back to the DB table
UPDATE_KEY = "api_fixture_id"

SCHEMA = "raw"
TABLE_NAME = "raw_fixtures"


 # Resolve the data directory for a given mode ('update' or 'full')
def resolve_directory(mode: str) -> Path:
    """
    Return the directory Path for the given mode: 'update' or 'full'.
    """
    if mode == "update":
        dir_path = Path(FIXTURES_UPDATE_DIR)
    elif mode == "full":
        dir_path = Path(CLEANED_DATA_DIR)
    else:
        raise ValueError(f"Unknown mode: {mode}")
    if not dir_path.exists():
        raise FileNotFoundError(f"Directory for mode '{mode}' does not exist: {dir_path}")
    return dir_path

 # Find the latest file for a given mode and pattern
def find_latest_file_for_mode(mode: str, pattern_template: str = "cleaned_fixtures_{mode}_*.parquet") -> Path:
    """
    Find the most recent file in the directory for the given mode matching the pattern.
    """
    dir_path = resolve_directory(mode)
    pattern = pattern_template.format(mode=mode)
    files = list(dir_path.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matching '{pattern}' in {dir_path}")
    return max(files, key=lambda p: p.stat().st_mtime)

 # Load the updates DataFrame with status and kickoff columns
def load_updates_df(update_parquet: str) -> pd.DataFrame:
    """
    Load the updates DataFrame from Parquet.
    
    Parameters
    ----------
    update_parquet : str
        Path to the Parquet file containing updated fixtures.
    
    Returns
    -------
    pd.DataFrame
        DataFrame with columns api_fixture_id, fixture_status, and kickoff_utc.
    """
    df_updates: pd.DataFrame = pd.read_parquet(update_parquet, columns=[UPDATE_KEY, "fixture_status", "kickoff_utc"])
    return df_updates

 # Load the base status DataFrame from the cleaned Parquet file
def load_base_status_df(cleaned_parquet: str) -> pd.DataFrame:
    """
    Load the base status DataFrame from the cleaned fixtures Parquet.
    
    Parameters
    ----------
    cleaned_parquet : str
        Path to the cleaned fixtures Parquet file.
    
    Returns
    -------
    pd.DataFrame
        DataFrame with columns api_fixture_id, fixture_status, and kickoff_utc.
    """
    return pd.read_parquet(cleaned_parquet, columns=[UPDATE_KEY, "fixture_status", "kickoff_utc"])

 # Get the list of fixture IDs that have changed (played or rescheduled)
def get_changed_ids(df_base_status: pd.DataFrame, df_updates: pd.DataFrame) -> list[int]:
    """
    Determine which fixture IDs have changed based on status or kickoff time.
    
    Parameters
    ----------
    df_base_status : pd.DataFrame
        DataFrame of base statuses with api_fixture_id, fixture_status, kickoff_utc.
    df_updates : pd.DataFrame
        DataFrame of update statuses with api_fixture_id, fixture_status, kickoff_utc.
    
    Returns
    -------
    list[int]
        List of fixture IDs that either changed to FT or were rescheduled.
    """
    df_merged = df_base_status.merge(df_updates, on=UPDATE_KEY, suffixes=("_base", "_upd"))
    # Keep only IDs where update status is 'FT' and base status isn't 'FT'
    mask_played = (df_merged["fixture_status_upd"] == "FT") & (df_merged["fixture_status_base"] != "FT")
    played_ids = df_merged.loc[mask_played, UPDATE_KEY].tolist()
    logger.info('%d fixtures changed to FT', len(played_ids))

    mask_rescheduled = df_merged["kickoff_utc_upd"] != df_merged["kickoff_utc_base"]
    rescheduled_ids = df_merged.loc[mask_rescheduled, UPDATE_KEY].tolist()
    logger.info('%d fixtures rescheduled', len(rescheduled_ids))

    changed_ids = list(set(played_ids) | set(rescheduled_ids))
    return changed_ids

 # Load filtered updates DataFrame for the changed fixture IDs
def load_filtered_updates(update_parquet: str, changed_ids: list[int]) -> pd.DataFrame:
    """
    Load only the updated rows for the given fixture IDs from the update Parquet.
    
    Parameters
    ----------
    update_parquet : str
        Path to the Parquet file containing updated fixtures.
    changed_ids : list[int]
        List of fixture IDs to load.
    
    Returns
    -------
    pd.DataFrame
        DataFrame filtered to only the specified fixture IDs.
    """
    return pd.read_parquet(update_parquet, filters=[(UPDATE_KEY, "in", changed_ids)])

# -- Helper: fetch target table columns once ----------------------------------
# Returns the list of column names for raw.raw_fixtures in physical order.
# We use this to (1) intersect with the DataFrame columns and (2) build
# a temp table with only the columns we actually intend to update.

def get_target_columns(cur) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        [SCHEMA, TABLE_NAME],
    )
    return [r[0] for r in cur.fetchall()]

def persist_updated_datasets(df_filtered: pd.DataFrame, cleaned_parquet: str) -> None:
    """
    Reload the full cleaned dataset, merge in updated rows, and write both
    the full and filtered DataFrames to their respective outputs.
    """
    try:
        start_persist = time.time()
        # Reload full dataset
        df_full = pd.read_parquet(cleaned_parquet)
        t_reload = time.time() - start_persist
        logger.info('Reloaded full dataset in %.2fs', t_reload)

        # Replace updated rows
        df_full = pd.concat([df_full, df_filtered]) \
                    .drop_duplicates(subset=UPDATE_KEY, keep='last') \
                    .reset_index(drop=True)
        t_merge = time.time() - start_persist - t_reload
        logger.info('Merged updated rows in %.2fs', t_merge)

        # Write out full dataset using write_outputs
        logger.info('Writing full dataset with %d rows to outputs', len(df_full))
        write_outputs(df_full, is_update=False)

        # Write out only the filtered updates
        logger.info('Writing filtered updates with %d rows to outputs', len(df_filtered))
        write_outputs(df_filtered, is_update=True)

        logger.info('Persisted datasets total in %.2fs', time.time() - start_persist)
    except Exception as e:
        logger.error('persist_updated_datasets failed: %s', e, exc_info=True)
        raise

def load_played_updates(
    updates_file: Optional[str] = None,
    cleaned_parquet: Optional[str] = None,
    page_size: int = 100
) -> int:
    """
    Load only the updated fixtures into the database by:
    1) Reading the Parquet file at updates_file for updated api_fixture_ids.
    2) Loading the full cleaned_parquet and filtering to those IDs.
    3) Batch-updating raw.raw_fixtures with all columns from the filtered DataFrame.

    Parameters
    ----------
    updates_file : str, optional
        Path to the Parquet file containing 'api_fixture_id' column.
        If None, defaults to the path defined by FIXTURES_UPDATE_DIR/cleaned_fixtures.parquet.
    cleaned_parquet : str, optional
        Path to the cleaned fixtures Parquet file.
        Defaults to CLEANED_DATA_DIR/cleaned_fixtures.parquet.
    page_size : int, optional
        Batch size for database updates.

    Returns
    -------
    int
        Number of fixture updates applied (0 if none or on error).

    Raises
    ------
    FileNotFoundError
        If the updates Parquet or cleaned Parquet file does not exist.
    OSError
        If any file I/O operation fails.
    """
    if updates_file:
        update_path = Path(updates_file)
    else:
        update_path = find_latest_file_for_mode("update")
    update_parquet = str(update_path)
    logger.info('Using updates file: %s', update_path)

    if cleaned_parquet:
        cleaned_path = Path(cleaned_parquet)
    else:
        cleaned_path = find_latest_file_for_mode("full")
    cleaned_parquet = str(cleaned_path)
    logger.info('Using cleaned fixtures file: %s', cleaned_path)

    # Load update DataFrame including status
    df_updates = load_updates_df(update_parquet)

    # Load base status DataFrame
    df_base_status = load_base_status_df(cleaned_parquet)

    # Get changed fixture IDs
    changed_ids = get_changed_ids(df_base_status, df_updates)

    if not changed_ids:
        logger.info('No fixtures changed; exiting.')
        return 0
    api_ids = changed_ids

    # Load updated rows from the update file itself
    start_load = time.time()
    df_filtered = load_filtered_updates(update_parquet, api_ids)
    logger.info('Loaded and filtered Parquet in %.2fs', time.time() - start_load)
    logger.info('DataFrame shape: %s, memory usage: %.1f MB', df_filtered.shape, df_filtered.memory_usage(deep=True).sum()/1e6)

    if df_filtered.empty:
        logger.info('No matching rows found in Parquet; exiting early.')
        return 0

    # Use a temporary table for bulk updates — updating only columns present in the DF
    # ------------------------------------------------------------------------------
    # 1) Discover target table columns and intersect with DF to get the columns
    #    we will actually update. The UPDATE_KEY must be present.
    temp_table = "tmp_fixture_updates"

    try:
        with get_db_connection() as conn, conn.cursor() as cur:
            # Ensure one explicit transaction; autocommit would drop TEMP table at creation time
            # when using ON COMMIT semantics. Keep autocommit off so temp table persists
            # through COPY + UPDATE steps.
            conn.autocommit = False

            # Get all columns from the destination table and intersect with DF
            tgt_cols = get_target_columns(cur)
            df_cols = [c for c in df_filtered.columns if c in tgt_cols]

            if UPDATE_KEY not in df_cols:
                raise ValueError(f"{UPDATE_KEY} missing from filtered updates DataFrame")

            # Ensure UPDATE_KEY is not part of the SET assignments
            update_cols = [c for c in df_cols if c != UPDATE_KEY]
            if not update_cols:
                logger.info('No updatable columns present besides the key; exiting.')
                return 0

            # 2) Create a temp table that inherits types by selecting from the target
            #    but only for the (key + update) columns. LIMIT 0 ensures no rows copied.
            create_temp_sql = sql.SQL(
                """
                CREATE TEMP TABLE {temp} AS
                SELECT {key_id}, {upd_cols}
                FROM {schema}.{table}
                LIMIT 0
                """
            ).format(
                temp=sql.Identifier(temp_table),
                key_id=sql.Identifier(UPDATE_KEY),
                upd_cols=sql.SQL(", ").join(sql.Identifier(c) for c in update_cols),
                schema=sql.Identifier(SCHEMA),
                table=sql.Identifier(TABLE_NAME),
            )

            temp_start = time.time()
            cur.execute(create_temp_sql)
            logger.info('Temp table %s created in %.2fs', temp_table, time.time() - temp_start)

            # 3) COPY only the (key + update) columns into the temp table in chunks.
            ordered_cols = [UPDATE_KEY] + update_cols
            copy_sql = sql.SQL("COPY {temp} ({cols}) FROM STDIN WITH CSV").format(
                temp=sql.Identifier(temp_table),
                cols=sql.SQL(", ").join(sql.Identifier(c) for c in ordered_cols),
            ).as_string(cur)

            # Build COALESCE-based SET pairs to avoid overwriting existing values with NULLs.
            # For each column c: tgt.c = COALESCE(src.c, tgt.c)
            set_pairs = sql.SQL(", ").join(
                sql.SQL("{col} = COALESCE(src.{col}, tgt.{col})").format(col=sql.Identifier(c))
                for c in update_cols
            )

            update_sql = sql.SQL(
                """
                UPDATE {schema}.{table} AS tgt
                SET {set_pairs}, updated_at = NOW()
                FROM {temp} AS src
                WHERE tgt.{key} = src.{key}
                """
            ).format(
                schema=sql.Identifier(SCHEMA),
                table=sql.Identifier(TABLE_NAME),
                set_pairs=set_pairs,
                temp=sql.Identifier(temp_table),
                key=sql.Identifier(UPDATE_KEY),
            )

            # Execute COPY + UPDATE per chunk
            for start in range(0, len(df_filtered), page_size):
                chunk = df_filtered.loc[:, ordered_cols].iloc[start : start + page_size]
                buf = StringIO()
                # Write CSV without header for COPY
                chunk.to_csv(buf, index=False, header=False)
                buf.seek(0)
                # NOTE: If autocommit were True or table created with ON COMMIT DROP,
                # the temp table would vanish before COPY, causing UndefinedTable.
                cur.copy_expert(copy_sql, buf)
                cur.execute(update_sql)
                conn.commit()
                logger.info('Applied %d updates for chunk [%d:%d]', len(chunk), start, start + len(chunk))

            # Explicitly drop temp table at the end (optional; it would disappear at session end)
            try:
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {temp}").format(temp=sql.Identifier(temp_table)))
            except Exception:
                logger.debug('Temp table %s drop skipped/failed (safe to ignore).', temp_table)

            # 4) Persist updated datasets to disk (full + filtered) for downstream use
            persist_updated_datasets(df_filtered, cleaned_parquet)

            return len(df_filtered)
    except (psycopg2.Error, OSError) as e:
        logger.error('Failed to apply updates: %s', e, exc_info=True)
        raise

def main():
    parser = argparse.ArgumentParser(description="Load played fixture updates into the database.")
    parser.add_argument("--updates-file", default=None, help="Path to the Parquet file of updates.")
    parser.add_argument("--parquet-file", default=None, help="Path to the cleaned fixtures Parquet file.")
    parser.add_argument("--page-size", type=int, default=100, help="Batch size for DB updates.")
    args = parser.parse_args()

    logger.info('Invoking load_played_updates')
    try:
        count = load_played_updates(args.updates_file, args.parquet_file, args.page_size)
        logger.info('Script finished: %d updates applied', count)
    except KeyboardInterrupt:
        logger.warning('KeyboardInterrupt received: stopping load_played_updates gracefully.')

if __name__ == "__main__":
    main()