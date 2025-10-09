from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import os
import etl.src.config as config
from etl.src.config import EXTRACT_FIXTURES_LOG
import psycopg2
import logging
import requests
import json
import time
from datetime import datetime, timedelta
from slugify import slugify
from typing import List, Dict, Any, Tuple, Optional
from etl.src.extract_metadata import get_db_connection
from etl.src.logger import get_logger
logger = get_logger(__name__, log_path=EXTRACT_FIXTURES_LOG)

try:
    from zoneinfo import ZoneInfo # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo # python < 3.9



# API connection parameters loaded from config.py.
API_KEY = config.API['key']
API_HOST = config.API['host']
API_URL = config.API['url']
HEADERS = {
    'x-rapidapi-key': API_KEY,
    'x-rapidapi-host': API_HOST
}

# Parameterized API endpoint names
FIXTURES_ENDPOINT = "fixtures"




# Helper to check for required fixture fields
def _validate_fixture(fixture: Dict[str, Any]) -> Tuple[List[str], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Check that a fixture dict has the required sections.
    Returns (missing_keys, fixture_data, teams_data, score_data).
    """
    fixture_data = fixture.get("fixture") or {}
    teams_data   = fixture.get("teams")   or {}
    score_data   = fixture.get("score")   or {}
    league_data  = fixture.get("league")  or {}
    missing = []
    if not fixture_data:
        missing.append("fixture")
    if not teams_data:
        missing.append("teams")
    if not score_data:
        missing.append("score")
    if not league_data: 
        missing.append("league")
    return missing, fixture_data, teams_data, score_data, league_data
    
def write_json(file_path: str, data: Any) -> None:
    """
    Write Python object `data` to a JSON file at `file_path`.
    Raises OSError or TypeError on failure.
    """
    try:       
        with open(file_path, "w") as file:
            json.dump(data, file, indent= 4)
            logger.info(f"Wrote JSON to {file_path} ({len(data)} records)")
    except (OSError, TypeError) as e :
        logger.error(f"Failed to write file {file_path}: {e}")
        raise 

def daily_sleep_calculator():
    """
    Sleeps until the next UTC day starts at midnight.
    This is useful for resetting daily rate limits.
    """
    utc_now = datetime.now(ZoneInfo("UTC"))
    next_day = datetime.combine(utc_now.date() + timedelta(days=1), datetime.min.time(), tzinfo=ZoneInfo("UTC"))
    sleep_sec = (next_day - utc_now).total_seconds()
    logger.info(f"Sleeping until next UTC day: {sleep_sec} seconds")
    time.sleep(sleep_sec)

# Unified rate-limited GET with retry/backoff and 429 handling
def _rate_limited_get(
    endpoint: str,
    params: Dict[str, Any],
    max_retries: int = 5
) -> List[Dict[str, Any]]:
    """
    Perform an HTTP GET with automatic rate-limit handling:
    - Honors daily and per-minute limit headers.
    - Retries on HTTP 429 with Retry-After header.
    - Uses exponential backoff on transient errors.
    Returns list of JSON 'response' elements or empty list on failure.
    """
    backoff = 1
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(f"{API_URL}{endpoint}", headers=HEADERS, params=params, timeout=10)
            # Enforce limits
            daily_rem = int(response.headers.get('x-ratelimit-requests-remaining', 1))
            min_rem   = int(response.headers.get('x-ratelimit-remaining', 1))
            if daily_rem < 1:
                # Sleep until next UTC day
                daily_sleep_calculator()
                continue
            if min_rem < 1:
                logger.info("Minute rate limit reached, sleeping 60s")
                time.sleep(60)
                continue

            # Check for HTTP errors
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', backoff))
                logger.info(f"HTTP 429 received, sleeping {retry_after}s")
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json().get("response", [])
        except (HTTPError, ConnectionError, Timeout) as e:
            logger.warning(f"Transient error on attempt {attempt}/{max_retries}: {e}. Retrying in {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except RequestException as e:
            logger.error(f"Non-retryable request error: {e}")
            break
    logger.error(f"Failed to fetch {endpoint} after {max_retries} attempts with params: {params}")
    return []

# endpoint params => Listof fixture
# endpoint is fixtures
#params is league_id and season

def fetch_fixtures(endpoint, params):
    """
    Fetch fixtures from the API with rate limiting and error handling.  
    Returns a list of fixtures or an empty list on failure.
    """
    return _rate_limited_get(endpoint, params)

#fixtures is a list of fixtures each fixture contains a lot of information that
#should be documented but i will only extract the ones i consider important

#extract each fixture from a league listof fixtures
def extract_fixtures_field(
    fixtures: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Transform raw fixture payloads into a list of simplified dicts.
    Skips any records missing critical data.
    """
    skip_count = 0
    extracted = []

    for fixture in fixtures:
        missing, fixture_data, teams_data, score_data, league_data = _validate_fixture(fixture)
        fid = fixture_data.get("id", "unknown")
        league_id = league_data.get("id", "unknown")
        season = league_data.get("season", "unknown")
        teams_names = (teams_data.get("home", {}).get("name"),teams_data.get("away", {}).get("name"))
        if missing:
            logger.warning(f"Skipping fixture id={fid} for teams {teams_data}: missing {missing}")
            skip_count += 1
            continue
        # All required data present; build record
        try:
            extracted.append({
                "api_fixture_id": fixture_data.get("id"),
                "api_league_id": league_data.get("id"),
                "season": league_data.get("season"),
                "kickoff_utc": fixture_data.get("date"),
                "fixture_status": fixture_data.get("status", {}).get("short", "NS"),
                "home_team_id": teams_data.get("home", {}).get("id"),
                "home_team_name": teams_data.get("home", {}).get("name"),
                "away_team_id": teams_data.get("away", {}).get("id"),
                "away_team_name": teams_data.get("away", {}).get("name"),
                "home_team_halftime_goal": score_data.get("halftime", {}).get("home"),
                "away_team_halftime_goal": score_data.get("halftime", {}).get("away"),
                "home_team_fulltime_goal": score_data.get("fulltime", {}).get("home"),
                "away_team_fulltime_goal": score_data.get("fulltime", {}).get("away")
            })
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Skipping bad fixture record id={fid} at index {i}: {e}")
            skip_count += 1
            continue
    if skip_count:
        logger.info(
            f"Skipped {skip_count} fixtures due to missing data or errors "
            f"league_id= {league_id, season}"
            f"teams names ={teams_names}"
        )
    return extracted

def save_fixture_data(
    country: str,
    league_name: str,
    season: int,
    data: List[Dict[str, Any]],
    filename: str = "fixtures.json",
    overwrite: bool = True
) -> None:
    """
    Save extracted fixture dicts to disk under the configured fixtures path.
    Creates directories as needed and respects the overwrite flag.
    """
    base_path = os.path.join(config.FIXTURES_PATH, slugify(country), slugify(league_name), str(season))
    os.makedirs(base_path, exist_ok=True)

    file_path = os.path.join(base_path, filename)

    if os.path.exists(file_path) and not overwrite:
        logger.info(f"Skipping save: {file_path} already exists.")
        return
    
    write_json(file_path, data)
    logger.info(f"Fixtures saved to: {file_path}")

def get_season_rows(
    cur: psycopg2.extensions.cursor
) -> List[Tuple[str, str, int, int]]:
    """
    Fetch all league-season combinations from the database.
    Returns a list of tuples (country_name, league_name, league_id, season_year).
    """
    cur.execute("""
        SELECT
          c.country_name,
          l.league_name,
          l.api_league_id,
          EXTRACT(YEAR FROM ls.start_date)::int AS season_year
        FROM dim.dim_league_seasons ls
        JOIN dim.dim_leagues l ON ls.league_id = l.league_id
        JOIN dim.dim_countries c ON l.country_id = c.country_id
    """)
    return cur.fetchall()

def extract_fixtures() -> Tuple[int, int]:
    """
    ETL for fixtures:
    - Reads api_league_id and season_year from dim tables
    - Fetches fixtures from API
    - Transforms and saves to disk
    """
    total_extracted = 0
    total_failed_leagues = 0
    # Measure DB connection time
    start_conn = time.time()
    conn = get_db_connection()
    conn_elapsed = time.time() - start_conn
    logger.info(f"Database connection established in {conn_elapsed:.2f}s")
    cur = conn.cursor()
    logger.info("Starting fixtures extraction")
    try:
        # Query each league-season combination
        rows = get_season_rows(cur)
        for country_name, league_name, api_league_id, season_year in rows:
            logger.info(f"Processing fixtures for {league_name} season {season_year}")
            try:
                # Fetch fixtures with timing
                start_fetch = time.time()
                fixtures = fetch_fixtures(FIXTURES_ENDPOINT, params={"league": api_league_id, "season": season_year})
                fetch_elapsed = time.time() - start_fetch
                logger.info(f"Fetched {len(fixtures)} fixtures for {league_name} season {season_year} in {fetch_elapsed:.2f}s")

                # Transform fixtures with timing
                start_transform = time.time()
                extracted = extract_fixtures_field(fixtures)
                transform_elapsed = time.time() - start_transform
                logger.info(f"Transformed {len(extracted)} fixtures in {transform_elapsed:.2f}s")

                if not extracted:
                    logger.warning(f"No fixtures for {league_name} ({season_year})")
                    continue

                # Save fixtures with timing
                start_save = time.time()
                save_fixture_data(country_name, league_name, season_year, extracted)
                save_elapsed = time.time() - start_save
                logger.info(f"Saved {len(extracted)} fixtures for {league_name} ({season_year}) in {save_elapsed:.2f}s")
                total_extracted += len(extracted)
            except (RequestException, psycopg2.Error) as e:
                logger.error(f"Network/DB error for {league_name} ({season_year}): {e}", exc_info=True)
                total_failed_leagues += 1
                continue
        logger.info("All fixture data extraction complete.")
        logger.info(
            f"Extraction summary: {total_extracted} fixtures extracted across "
            f"{len(rows)} league-seasons with {total_failed_leagues} failures"
        )
        return total_extracted, total_failed_leagues
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":

    logger.info("Invoking extract_fixtures")
    try:
        extract_fixtures()
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt received: shutting down extraction.")
