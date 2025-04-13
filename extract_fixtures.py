import os
import logging
import requests
import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from slugify import slugify


API_URL = "https://v3.football.api-sports.io/"
HEADERS = {
    'x-rapidapi-key': 'fc243408e4f79d927707f13d0de433a5',
    'x-rapidapi-host': 'v3.football.api-sports.io'
}

logging.basicConfig(
    filename= "fixtures_logs.txt",
    level= logging.DEBUG,
    format= "%(asctime)s - %(levelname)s: %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S"
)


def load_json(file_path):
    with open(file_path, 'r') as file:
        json_file = json.load(file)
        logging.info("json file has been loaded into program")
        return json_file
    
def write_json(file_path, data):
    try:       
        with open(file_path, "w") as file:
            json.dump(data, file, indent= 4)
    except Exception as e :
        logging.error(f"Failed to write file {file_path}: {e}")
        raise 

metadata = load_json('metadata_with_league_ids.json')


# endpoint params => Listof fixture
# endpoint is fixtures
#params is league_id and season

def fetch_fixtures(endpoint, params):
    try:
        response = requests.get(f"{API_URL}{endpoint}", headers= HEADERS, params=params)

        #Access rate limit headers safely
        daily_remaining_limit = int(response.headers.get('x-ratelimit-requests-remaining', 1))
        minute_remaining_limit = int(response.headers.get('x-ratelimit-remaining', 1))
        minute_wait = 60 #seconds

        #Daily limit handling
        if  daily_remaining_limit < 1:
            utc_now = datetime.now(ZoneInfo("UTC"))
            next_day = datetime.combine(utc_now.date() + timedelta(days=1), datetime.min.time(), tzinfo=ZoneInfo("UTC"))
            wait_seconds = (next_day - utc_now).total_seconds()
            logging.info("Daily request rate limit hit. Sleeping until next day UTC time")
            time.sleep(wait_seconds)
            
            #Retry once after waiting
            return fetch_fixtures(endpoint, params)
            

        # Per-minute limit handling
        if minute_remaining_limit < 1:
            logging.info(f"Minute request rate limit hit. Sleeping for 1 minute")
            time.sleep(minute_wait)
            
            #Retry once after waiting
            return fetch_fixtures(endpoint, params)
            

        response.raise_for_status()

        data = response.json().get("response",[])
        logging.debug(f"Response headers: {dict(response.headers)}")

        if not data:
            logging.warning(f"No fixtures found for the params: {params}")
            return []
    
        return data

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error: {e} for endpoint {endpoint}")
    except requests.exceptions.ConnectionError:
        logging.error("Network error: Could not connect to server.")
    except requests.exceptions.Timeout:
        logging.error("Request Timeout")
    except requests.exceptions.RequestException as e:
        logging.error(f"Unexpected error: {e} for endpoint {endpoint}")

    return []

#fixtures is a list of fixtures each fixture contains a lot of information that
#should be documented but i will only extract the ones i consider important


def extract_fixtures_field(fixtures, league_id, season):
    extracted = []

    for fixture in fixtures:
        try:
            extracted.append({
            "fixture_id": fixture.get("fixture", {}).get("id", None),
            "league_id": league_id,
            "season": season,
            "date": fixture.get("fixture", {}).get("date", None),
            "status": fixture.get("fixture", {}).get("status", {}).get("short", "NS"),
            "home_team_id": fixture.get("teams", {}).get("home", {}).get("id", None),
            "home_team_name": fixture.get("teams", {}).get("home", {}).get("name", None),
            "away_team_id": fixture.get("teams", {}).get("away", {}).get("id", None),
            "away_team_name": fixture.get("teams", {}).get("away", {}).get("name", None),
            "home_team_halftime_goal": fixture.get("score", {}).get("halftime", {}).get("home", None),
            "away_team_halftime_goal": fixture.get("score", {}).get("halftime", {}).get("away", None),
            "home_team_fulltime_goal": fixture.get("score", {}).get("fulltime", {}).get("home", None),
            "away_team_fulltime_goal": fixture.get("score", {}).get("fulltime", {}).get("away", None)
            })
        except Exception as e:
            logging.warning(f"Skipping bad fixture record: {e}")
            continue
    return extracted

def save_fixture_data(country, league_name, season, data, filename="fixtures.json", overwrite=False):
    """
    The function takes the country, league_name, season and extracted fixtures 
    and stores it in a folder path for each country -> league -> year
    """
    base_path = f"./data/fixtures/{slugify(country)}/{slugify(league_name)}/{season}"
    os.makedirs(base_path, exist_ok=True)

    file_path = os.path.join(base_path, filename)

    if os.path.exists(file_path) and not overwrite:
        logging.info(f"Skipping save: {file_path} already exists.")
        return
    
    write_json(file_path, data)
    logging.info(f"Fixtures saved to: {file_path}")

for country in metadata:
    for league in country['leagues']:
        #check if league_id is missing
        if "league_id" not in league or not league["league_id"]:
            logging.warning(f"Skipping league '{league['name']}' - missing league_id.")
            continue

        try:
            id= league["league_id"]
            latest_season = max( season['start_year'] for season in league['seasons'])

        #fetch fixtures
            fixtures = fetch_fixtures("fixtures", params={"league": id, "season": latest_season})

            extracted_fixtures = extract_fixtures_field(fixtures, id, latest_season)

            if not extracted_fixtures:
                logging.warning(f"No fixtures extracted for {league['name']} ({latest_season})")
                continue

            save_fixture_data(country['name'],league['name'], latest_season, extracted_fixtures)

            logging.info(f"Saved {len(extracted_fixtures)} fixtures for {league['name']} ({latest_season})")
        
        except Exception as e:
            logging.error(f"Error while processing league '{league['name']}': {e}")
            continue

logging.info("All fixture data extraction complete.")

    

