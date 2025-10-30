"""
Module to update played fixtures in raw.raw_fixtures by fetching missing results from the API.
- Groups league-season and fetches fixtures since earliest missing date.
- Updates home and away goals and fixture status for fixtures with null fulltime goals.
"""

import os
import json
import time
import argparse

from etl.src.config import UPDATE_FIXTURES_LOG, FIXTURE_UPDATES_JSON
from etl.src.extract_fixtures import fetch_fixtures, FIXTURES_ENDPOINT, extract_fixtures_field, _validate_fixture
from etl.src.extract_metadata import get_db_connection
from typing import List, Dict, Any, Optional

from etl.src.logger import get_logger
from datetime import date
logger = get_logger(__name__, log_path=UPDATE_FIXTURES_LOG)


# Helper to write updates JSON atomically
def _write_updates_json(updates: List[Dict[str, Any]], output_path: str) -> None:
    """
    Write the list of update dicts to a JSON file atomically.
    """
    tmp_path = output_path + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(updates, f, default=str, indent=2)
        os.replace(tmp_path, output_path)
        logger.info(f"Wrote {len(updates)} fixture updates to {output_path}")
    except (OSError, TypeError) as e:
        logger.error(f"Failed to write updates JSON at {output_path}: {e}", exc_info=True)
        raise

def to_update_fixture_ids() -> List[str]:
    """Extract fixture IDs that need updating."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT srf.api_fixture_id
            FROM raw_stg.stg_raw_fixtures as srf
            JOIN raw_stg.stg_dim_leagues l
            ON srf.api_league_id = l.api_league_id
            JOIN raw_stg.stg_dim_league_seasons ls
            ON l.league_id = ls.league_id
	            AND ls.season = srf.season
            WHERE srf.kickoff_utc < NOW() - INTERVAL '2 hour'
                AND (srf.home_team_fulltime_goal IS NULL OR srf.away_team_fulltime_goal IS NULL)
	            AND ls.is_current
        """)
        ids = [str(row[0]) for row in cur.fetchall()]
        logger.info(f"Identified {len(ids)} fixture IDs needing updates.")
        return ids
    except Exception as e:
        logger.error(f"Error querying fixture IDs needing updates: {e}", exc_info=True)
        return []
    finally:
        conn.close()

def update_played_fixtures(to_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Batch-update fixtures in raw.raw_fixtures that lack fulltime goals by fetching
    updated data from the API, using the earliest missing kickoff date per league-season.

    Parameters
    ----------
    output_filename : str, optional
        Path to the JSON file to write. If None, defaults to
        the path defined by FIXTURE_UPDATES_JSON in config.
    to_date : str, optional
        The 'to' date (ISO format) for fetching fixtures. Defaults to today's date.
    Returns
    -------
    int
        Number of fixture updates applied (0 on error or no updates).
    """
    start_total = time.time()
    logger.info("Starting update_played_fixtures")
    updated_fixtures = []
    if to_date is None:
        to_date = date.today().isoformat()
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1) Determine league-season groups
        cur.execute("""
            SELECT
                ls.league_season_id,
                l.api_league_id,
                ls.season,
                MIN(srf.kickoff_utc)::date AS from_date
            FROM raw_stg.stg_raw_fixtures srf
            JOIN raw_stg.stg_dim_leagues l
                    ON srf.api_league_id = l.api_league_id
            JOIN raw_stg.stg_dim_league_seasons ls
              ON l.league_id = ls.league_id
              AND ls.season = srf.season
            WHERE
              srf.kickoff_utc < NOW() - INTERVAL '2 hour'
              AND (srf.home_team_fulltime_goal IS NULL OR srf.away_team_fulltime_goal IS NULL)
                And ls.is_current
            GROUP BY ls.league_season_id, l.api_league_id, ls.season

        """)
        to_update = cur.fetchall()
        if not to_update:
            logger.info("No fixtures need updating; exiting early.")
            return 0
        logger.info(f"Found {len(to_update)} league-season groups to refresh")

        
        # 2) For each league-season, fetch fixtures since the earliest missing date
        for league_season_id, api_league_id, season, from_date in to_update:
            # Fetch fixtures with built-in rate limiting
            start_req = time.time()
            try:
                fixtures = fetch_fixtures(
                    FIXTURES_ENDPOINT,
                    {
                        "league": api_league_id,
                        "season": season,
                        "from": from_date.isoformat(),
                        "to": to_date
                    }
                )
                fetch_fixtures_elapsed = time.time() - start_req
                logger.info(
                f"Fetched {len(fixtures)} fixtures for league {league_season_id} "
                f"(API id {api_league_id}), season {season} in {fetch_fixtures_elapsed:.2f}s"
                )
            except Exception as e:
                logger.error(
                    f"Error fetching fixtures for league_season_id:  {league_season_id} "
                    f"(API id {api_league_id}), season {season}: {e}",
                    exc_info=True
                )
                continue
            # 3) Extract updated fixtures batch
            try:
                extracted_batch = extract_fixtures_field(fixtures)
                updated_fixtures.extend(extracted_batch)
                logger.info(
                    f"Extracted {len(extracted_batch)} updated fixtures for league_season_id:  {league_season_id} "
                    f"(API id {api_league_id}), season {season}"
                )
            except Exception as e:
                logger.error(
                    f"Error extracting fixtures for league_season_id:  {league_season_id} (API id {api_league_id}), season {season}: {e}",
                    exc_info=True
                )
                continue    
            
            
            elapsed = time.time() - start_req
            
            logger.info(f"Updated a total of {len(extracted_batch)} in a batch of fixtures in {elapsed:.2f}s ")

        

        if not updated_fixtures:
            logger.info("No fixture updates to write; exiting early.")
            return []
        
        logger.info(f"Total of {len(updated_fixtures)} fixtures updated across all league-seasons.")
        #----------------------------------------------------------------
        #ids = [f['fixture_id'] for f in updated_fixtures if _validate_fixture(f)]
        """ use to check ids not in the to update list"""
        #----------------------------------------------------------------
        return updated_fixtures

        # 4) Write updates to JSON for downstream processing
        

    except Exception as e:
        logger.error(f"Error during update_played_fixtures: {e}", exc_info=True)
        return []

    finally:
        conn.close()
        total_elapsed = time.time() - start_total
        logger.info(f"Finished update_played_fixtures in {total_elapsed:.2f}s")
        logger.info(f"Updated a total of {len(updated_fixtures)} fixtures.")


def update_by_ids(ids: List[int]) -> List[Dict[str, Any]]:
    """
    Update specific fixtures by their fixture IDs.

    Parameters
    ----------
    ids : List[int]
        List of fixture IDs to update.

    Returns
    -------
    List[Dict[str, Any]]
        List of updated fixture dictionaries.
    """


    """
    TODOS:
    ADD timing
    """
    if not ids:
        logger.info("No fixture IDs provided for update; exiting early.")
        return []

    updated_fixtures = []
    fixtures = []
    logger.info(f"Starting update_by_ids for {len(ids)} fixture IDs")
    # Fetch fixtures with built-in rate limiting
    start_req = time.time()
    ptr= 0
    batch_size = 20 # Adjust batch size as needed
    while ptr < len(ids):
        batch_ids = ids[ptr:ptr+batch_size]
        batch_ids = "-".join(batch_ids)
        logger.info(batch_ids)
        try:
            batch_fixtures = fetch_fixtures(
            FIXTURES_ENDPOINT,
                {
                    "ids": batch_ids
                }
            )
            fetch_fixtures_elapsed = time.time() - start_req
            logger.info(
                f"Fetched {len(batch_fixtures)} fixtures for provided IDs in {fetch_fixtures_elapsed:.2f}s"
            )
            fixtures.extend(batch_fixtures)
            ptr += batch_size
        except Exception as e:
            logger.error(
                f"Error fetching fixtures for provided IDs: {e} in batch {ptr// batch_size}",
                exc_info=True
            )
            continue

    try:
        # 2) Extract updated fixtures batch
        extracted_batch = extract_fixtures_field(fixtures)

        if not extracted_batch:
            logger.info("No fixture updates extracted for provided IDs; exiting early.")
            return []
        
        updated_fixtures.extend(extracted_batch)
        logger.info(
            f"Extracted {len(extracted_batch)} updated fixtures for provided IDs"
        )
        return updated_fixtures
    except Exception as e:  
        logger.error(
            f"Error extracting fixtures for provided IDs: {e}",
            exc_info=True
        )
        return []
    
    
def to_json(updated_fixtures: List[Dict[str, Any]], output_filename: str) -> None:
    """
    Write the list of fixture updates to a JSON file.

    Parameters
    ----------
    data : List[Dict[str, Any]]
        List of fixture update dictionaries.
    output_file : str
        Path to the JSON file to write.
    """
    if output_filename:
            output_path = output_filename
    else:
        output_path = FIXTURE_UPDATES_JSON
    _write_updates_json(updated_fixtures, output_path)
    return len(updated_fixtures)



def update_fixtures_main(output_filename: Optional[str] = None) -> None:
    """
    Main function to update played fixtures and write updates to JSON.
    """
    try:
        ids = to_update_fixture_ids()
        updated_fixtures = update_by_ids(ids)
        count = to_json(updated_fixtures, output_filename)
        logger.info(f"update_fixtures_main finished: {count} updates applied.")
    except Exception as e:
        logger.error(f"Error in update_fixtures_main: {e}", exc_info=True)
    
    finally:
        logger.info("update_fixtures_main completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch and write played fixture updates to JSON."
    )
    parser.add_argument(
        "--earliest-kickoff",
        action="store_true",
        help="If set, update fixtures by specific IDs needing updates instead of by league-season."
    )
    parser.add_argument(
        "--output-file",
        dest="output_file",
        type=str,
        help="Path to write the updates JSON (defaults to FIXTURE_UPDATES_JSON)."
    )
    parser.add_argument(
        "--to-date",
        dest="to_date",
        type=str,
        help="ISO date (YYYY-MM-DD) to fetch fixtures up to (defaults to today)."
    )
    args = parser.parse_args()

    logger.info("Invoking update_played_fixtures (output_file=%s, to_date=%s)",
                args.output_file, args.to_date)
    try:
        if args.earliest_kickoff:
            updated_fixtures = update_played_fixtures(
                to_date=args.to_date
            )
            
        else:
            update_fixtures_main(output_filename=args.output_file)
            
        
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt received: shutting down update_played_fixtures gracefully.")
