from etl.src.config import TRANSFORM_FIXTURES_LOG, CLEANED_DATA_DIR, FIXTURES_UPDATE_DIR, FIXTURES_PATH, FIXTURE_UPDATES_JSON
import pandas as pd
import numpy as np
from pathlib import Path
from etl.src.logger import get_logger
import time
from typing import List
from datetime import datetime
import os
import argparse
logger = get_logger(__name__, log_path=TRANSFORM_FIXTURES_LOG)

REQUIRED_COLS = [
            "api_fixture_id",
            "api_league_id",
            "season",
            "kickoff_utc",
            "fixture_status",
            "home_team_id",
            "home_team_name",
            "away_team_id",
            "away_team_name",
            "home_team_halftime_goal",
            "away_team_halftime_goal",
            "home_team_fulltime_goal",
            "away_team_fulltime_goal"
        ]

def load_full_fixtures(base_dir: str = FIXTURES_PATH) -> pd.DataFrame:
    """
    Load and concatenate all fixtures.json files under `base_dir` (full fixtures).
    Returns a DataFrame of all fixture records.
    """
    start_load = time.time()
    #Hold the fixtures from different leagues as iterables
    all_fixtures = []

    #Find all fixtures.json files in subfolders using Path.rglob
    json_file_paths = [str(p) for p in Path(base_dir).rglob("fixtures.json")]
    logger.debug(f"Found {len(json_file_paths)} fixture files in {base_dir}")

    for file_path in json_file_paths:
        try:
            # Extract country, league, and season from the folder path
            p = Path(file_path)
            season = p.parent.name
            league = p.parent.parent.name
            country = p.parent.parent.parent.name
            
            # load fixture json to pandas 
            df = pd.read_json(file_path)

            df['season'] = season

            all_fixtures.append(df)
            logger.debug(f"Loaded {file_path} with {len(df)} fixtures")

        except (OSError, ValueError) as e:
            logger.warning(f"Failed to load {file_path}: {e}")
    if not all_fixtures:
        logger.warning("No fixture files loaded in full mode")
        logger.info('load_full_fixtures took %.2fs (no files)', time.time() - start_load)
        return pd.DataFrame()
    elapsed_load = time.time() - start_load
    logger.info('load_full_fixtures took %.2fs', elapsed_load)
    return pd.concat(all_fixtures, ignore_index=True)

def load_updated_fixtures(update_json: str = FIXTURE_UPDATES_JSON) -> pd.DataFrame:
    """
    Load updated fixtures from a single JSON file.
    Returns a DataFrame of all updated fixture records.
    """
    start_update_load = time.time()
    if not Path(update_json).exists():
        logger.warning(f"No fixture update JSON found at {update_json}")
        logger.info('load_updated_fixtures took %.2fs (no file)', time.time() - start_update_load)
        return pd.DataFrame()
    try:
        df = pd.read_json(update_json, orient='records')
        elapsed_update_load = time.time() - start_update_load
        logger.info('load_updated_fixtures took %.2fs', elapsed_update_load)
        logger.info(f"Loaded update fixtures from {update_json} with {len(df)} records")
        return df
    except (OSError, ValueError) as e:
        logger.warning(f"Failed to load update fixtures from {update_json}: {e}")
        elapsed_update_load = time.time() - start_update_load
        logger.info('load_updated_fixtures took %.2fs', elapsed_update_load)
        return pd.DataFrame()



def validate_schema(df: pd.DataFrame, required_cols: List[str]) -> bool:
    """
    Validate that required columns exist in the DataFrame.
    Logs an error if any are missing.
    Returns True if schema is valid, False otherwise.
    """
    missing = set(required_cols) - set(df.columns)
    if missing:
        logger.error(f"Missing required columns: {missing}")
        return False
    return True


# New helper: cast numeric columns to nullable Int64
def cast_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast ID, season, and goal columns to nullable Int64.
    """
    int_cols = [
        "api_fixture_id",
        "api_league_id",
        "season",
        "home_team_id",
        "away_team_id",
        "home_team_halftime_goal",
        "away_team_halftime_goal",
        "home_team_fulltime_goal",
        "away_team_fulltime_goal",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df

def handle_special_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute fulltime and halftime results for walkover (WO) and awarded (AWD) fixtures.
    For WO/AWD: compute fulltime result if scores present; compute halftime only if scores present.
    """
    status = df["fixture_status"].astype(str).str.upper()
    special_mask = status.isin(["WO", "AWD"])

    # Fulltime for special
    h_ft = df["home_team_fulltime_goal"]
    a_ft = df["away_team_fulltime_goal"]
    valid_ft = special_mask & h_ft.notna() & a_ft.notna()

    df.loc[valid_ft & (h_ft > a_ft), ["home_fulltime_result", "away_fulltime_result"]] = ["win", "loss"]
    df.loc[valid_ft & (h_ft < a_ft), ["home_fulltime_result", "away_fulltime_result"]] = ["loss", "win"]
    df.loc[valid_ft & (h_ft == a_ft), ["home_fulltime_result", "away_fulltime_result"]] = ["draw", "draw"]

    # Halftime for special only if both present
    h_ht = df["home_team_halftime_goal"]
    a_ht = df["away_team_halftime_goal"]
    valid_ht = special_mask & h_ht.notna() & a_ht.notna()

    df.loc[valid_ht & (h_ht > a_ht), ["home_halftime_result", "away_halftime_result"]] = ["win", "loss"]
    df.loc[valid_ht & (h_ht < a_ht), ["home_halftime_result", "away_halftime_result"]] = ["loss", "win"]
    df.loc[valid_ht & (h_ht == a_ht), ["home_halftime_result", "away_halftime_result"]] = ["draw", "draw"]

    return df


def compute_game_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute results ('win', 'loss', 'draw') for fixtures based on goals.
    Applies to all non-abandoned fixtures, including WO and AWD.
    """
    # Only compute for fixtures with full-time status 'FT'
    status = df["fixture_status"].astype(str).str.upper()
    ft_status_mask = status.eq("FT")

    # Fulltime results
    h_ft = df["home_team_fulltime_goal"]
    a_ft = df["away_team_fulltime_goal"]
    valid_ft = ft_status_mask & h_ft.notna() & a_ft.notna()

    # Compute vectorized
    df.loc[valid_ft, "home_fulltime_result"] = np.where(
        h_ft[valid_ft] > a_ft[valid_ft], "win",
        np.where(h_ft[valid_ft] < a_ft[valid_ft], "loss", "draw")
    )
    df.loc[valid_ft, "away_fulltime_result"] = np.where(
        h_ft[valid_ft] > a_ft[valid_ft], "loss",
        np.where(h_ft[valid_ft] < a_ft[valid_ft], "win", "draw")
    )

    # Halftime results
    h_ht = df["home_team_halftime_goal"]
    a_ht = df["away_team_halftime_goal"]
    valid_ht = ft_status_mask & h_ht.notna() & a_ht.notna()

    # Compute vectorized
    df.loc[valid_ht, "home_halftime_result"] = np.where(
        h_ht[valid_ht] > a_ht[valid_ht], "win",
        np.where(h_ht[valid_ht] < a_ht[valid_ht], "loss", "draw")
    )
    df.loc[valid_ht, "away_halftime_result"] = np.where(
        h_ht[valid_ht] > a_ht[valid_ht], "loss",
        np.where(h_ht[valid_ht] < a_ht[valid_ht], "win", "draw")
    )

    return df


def compute_results(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute fulltime and halftime results, handling abandoned, walkover, and awarded fixtures.
    """
    # Initialize result columns to None
    for col in [
        "home_fulltime_result", "away_fulltime_result",
        "home_halftime_result", "away_halftime_result"
    ]:
        df[col] = None
    # Compute for walkover and awarded
    df = handle_special_results(df)
    # Compute for normal games
    df = compute_game_results(df)
    return df

def load_fixtures(is_update: bool) -> pd.DataFrame:
    """
    Load fixtures based on mode.
    Parameters
    ----------
    is_update : bool
        If True, load updated fixtures; otherwise load all fixtures.
    Returns
    -------
    pd.DataFrame
        Loaded fixtures DataFrame (empty if none).
    """
    if is_update:
        logger.info("Running fixtures transformation in update mode")
        df_fixtures = load_updated_fixtures()
        logger.info(f"Loaded {len(df_fixtures)} fixtures for update")
        return df_fixtures
    else:
        logger.info("Running fixtures transformation in full mode")
        df_fixtures = load_full_fixtures()
        logger.info(f"Loaded {len(df_fixtures)} fixtures in full mode")
        return df_fixtures

def select_output_dir(is_update: bool) -> str:
    """
    Select output directory based on mode.
    Parameters
    ----------
    is_update : bool
        If True, return the update directory; otherwise return the clean data directory.
    Returns
    -------
    str
        Path to the output directory.
    """
    if is_update:
        return FIXTURES_UPDATE_DIR
    else:
        return CLEANED_DATA_DIR


def log_upcoming_stats(df: pd.DataFrame) -> None:
    """
    Log count of upcoming fixtures (no fulltime result).
    """
    upcoming_count = df["home_fulltime_result"].isna().sum()
    logger.info(f"Upcoming fixtures: {upcoming_count}")

def write_outputs(df: pd.DataFrame, is_update: bool) -> None:
    """
    Write cleaned fixtures to the appropriate directory with dynamic filenames.
    """
    # Determine output directory
    out_dir = Path(select_output_dir(is_update))
    logger.debug(f'Debug: writing outputs to directory {out_dir}')
    out_dir.mkdir(parents=True, exist_ok=True)

    # Build dynamic filenames based on mode and timestamp
    mode = 'update' if is_update else 'full'
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f'cleaned_fixtures_{mode}_{ts}.csv'
    parquet_filename = f'cleaned_fixtures_{mode}_{ts}.parquet'

    csv_path = out_dir / csv_filename
    parquet_path = out_dir / parquet_filename

    try:
        start_write = time.time()
        # Atomic CSV write
        tmp_csv = str(csv_path) + '.tmp'
        df.to_csv(tmp_csv, index=False)
        os.replace(tmp_csv, str(csv_path))
        logger.info('Cleaned fixtures CSV written to %s', csv_path)

        # Atomic Parquet write
        tmp_parquet = str(parquet_path) + '.tmp'
        df.to_parquet(tmp_parquet, index=False)
        os.replace(tmp_parquet, str(parquet_path))
        logger.info('Cleaned fixtures Parquet written to %s', parquet_path)
        elapsed_write = time.time() - start_write
        logger.info('write_outputs total duration: %.2fs', elapsed_write)
    except Exception as e:
        logger.error('Failed to write outputs to %s: %s', out_dir, e, exc_info=True)


def transform_fixtures(is_update: bool = False) -> int:
    """
    Main transformation:
    - Loads raw fixtures from disk
    - Computes home/away results for fulltime and halftime
    - Converts scores to nullable integers
    - Writes cleaned CSV and Parquet to the base data directory

    Returns
    -------
    int
        Number of fixtures processed and transformed.
    """
    logger.info("Starting fixtures transformation")
    start_time = time.time()
    try:
        # Load
        t0 = time.time()
        df_fixtures = load_fixtures(is_update)
        load_dur = time.time() - t0
        logger.info(f"Loaded {len(df_fixtures)} fixtures in {load_dur:.2f}s (mode={'update' if is_update else 'full'})")
        if is_update:
            logger.debug("First few rows of update data:\n%s", df_fixtures.head().to_string())

        if df_fixtures.empty:
            logger.warning("No fixtures to transform; exiting.")
            return 0

        # Schema validation
        if not validate_schema(df_fixtures, REQUIRED_COLS):
            return 0

        # Cast
        t1 = time.time()
        df_fixtures = cast_numeric_columns(df_fixtures)
        logger.debug(f"Casting numeric columns took {(time.time() - t1):.2f}s")

        # Compute results
        t2 = time.time()
        df_fixtures = compute_results(df_fixtures)
        logger.debug(f"Computing results took {(time.time() - t2):.2f}s")

        # Log stats
        log_upcoming_stats(df_fixtures)

        # Write outputs
        t3 = time.time()
        write_outputs(df_fixtures, is_update)
        logger.debug(f"Writing outputs took {(time.time() - t3):.2f}s")

        # Final summary
        total_dur = time.time() - start_time
        logger.info(f"Transformation complete: {len(df_fixtures)} rows processed in {total_dur:.2f}s")
        return len(df_fixtures)
    except (ValueError, OSError) as e:
        elapsed = time.time() - start_time
        logger.info(f"Transformation aborted after {elapsed:.2f} seconds due to error")
        logger.error(f"Error during fixtures transformation: {e}", exc_info=True)
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform fixture data into cleaned output.")
    parser.add_argument(
        "--update",
        action="store_true",
        help="Run in update mode (process only updated fixtures)."
    )
    args = parser.parse_args()

    logger.info("Invoking transform_fixtures (update mode=%s)", args.update)
    try:
        transform_fixtures(is_update=args.update)
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received: shutting down transformation gracefully.")
