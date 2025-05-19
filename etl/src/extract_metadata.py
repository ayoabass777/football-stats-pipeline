import os
import etl.src.config as config
import logging
import requests
import json
import pprint
# fetching tools
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
#
"""
This file is to extract leageu_id and team_id
from the API. 
Storing the id's in metadata makes it easier, 
direct and consistent to fetch data from API
"""

logging.basicConfig( 
    filename= './data/logs/extract_metadata_logs.txt',
    level= logging.DEBUG,
    format= "%(asctime)s - %(levelname)s: %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S"
    )

API_KEY = config.API['key']
API_HOST = config.API['host']
API_URL = config.API['url']
HEADERS = {
    'x-rapidapi-key': API_KEY,
    'x-rapidapi-host': API_HOST
}



def load_json(file_path):
    try:
        with open(file_path, 'r') as file:
            json_file = json.load(file)
            logging.info("json file has been loaded into program")
            return json_file
    except Exception as e:
        logging.error(f"Failed to load {file_path}: {e}")
        raise
    
def write_json(file_path, data):
    try:       
        with open(file_path, "w") as file:
            json.dump(data, file, indent= 4)
    except Exception as e :
        logging.error(f"Failed to write file {file_path}: {e}")
        raise 

# endpoint params => league_id
# endpoint is the leagues api endpoint
# params is a dictionary of {"country": "String", "name": "String"}
# country is the name of the country
# name  is the name of the league

#fetch the league_id of a league from a given country
# def fetch_leagueid(leagues, params): pass

def fetch_leagueid(endpoint, params):

    def handle_daily_limit(request_allowed):
                utc_now = datetime.now(ZoneInfo("UTC"))
                next_day = datetime.combine(utc_now.date() + timedelta(days=1), datetime.min.time(), tzinfo=ZoneInfo("UTC"))
                wait_seconds = (next_day - utc_now).total_seconds()
                logging.info("Daily request rate limit hit. Sleeping until next day UTC time")
                time.sleep(wait_seconds)
            
            #Retry once after waiting
                return fetch_teams(endpoint, params)
    
    def handle_minute_limit(request_allowed):
        minute_wait = 60 #seconds

        logging.info(f"Minute request rate limit hit. Sleeping for 1 minute")
        time.sleep(minute_wait)
            
        #Retry once after waiting
        return fetch_teams(endpoint, params)
    
    try:
        response = requests.get(f"{API_URL}{endpoint}", headers= HEADERS, params= params)

         #Access rate limit headers safely
        daily_remaining_request_allowed = int(response.headers.get('x-ratelimit-requests-remaining', 1))
        minute_remaining_request_allowed = int(response.headers.get('x-ratelimit-remaining', 1))
        

        #Daily limit handling
        if  daily_remaining_request_allowed < 1:
            handle_daily_limit(daily_remaining_request_allowed )
                
        # Per-minute limit handling
        if minute_remaining_request_allowed < 1:
            handle_minute_limit(minute_remaining_request_allowed)
        
        #raise HTTP error (if status_code is 4xx or 5xx)
        response.raise_for_status()

        data = response.json().get("response", [])
        
        if not data:
            logging.warning(f"No league found for params: {params}")
            return []
        
        return data[0].get("league", {}).get("id", None)
    
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error: {e} for endpoint {endpoint}")
    except requests.exceptions.ConnectionError:
        logging.error("Network error: Could not connect to server.")
    except requests.exceptions.Timeout:
        logging.error("Request Timeout")
    except requests.exceptions.RequestException as e:
        logging.error(f"Unexpected error: {e} for endpoint {endpoint}")

    return []


# endpoint params => team_name, team_id
# fetch a list of team_name and team_id for the teams in a given league and season 
# where season is_current
# endpoint is the teams api endpoint
# params is a dictionary of {"league": INT, "season": INT}
# league is the league_id
# season is the start_year of the season

def fetch_teams(endpoint, params):

    def handle_daily_limit(request_allowed):
                utc_now = datetime.now(ZoneInfo("UTC"))
                next_day = datetime.combine(utc_now.date() + timedelta(days=1), datetime.min.time(), tzinfo=ZoneInfo("UTC"))
                wait_seconds = (next_day - utc_now).total_seconds()
                logging.info("Daily request rate limit hit. Sleeping until next day UTC time")
                time.sleep(wait_seconds)
            
            #Retry once after waiting
                return fetch_teams(endpoint, params)
    
    def handle_minute_limit(request_allowed):
        minute_wait = 60 #seconds

        logging.info(f"Minute request rate limit hit. Sleeping for 1 minute")
        time.sleep(minute_wait)
            
        #Retry once after waiting
        return fetch_teams(endpoint, params)
    
    try:
        response =  requests.get(f"{API_URL}{endpoint}", params= params, headers= HEADERS)
        
      #Access rate limit headers safely
        daily_remaining_request_allowed = int(response.headers.get('x-ratelimit-requests-remaining', 1))
        minute_remaining_request_allowed = int(response.headers.get('x-ratelimit-remaining', 1))
        

        #Daily limit handling
        if  daily_remaining_request_allowed < 1:
            handle_daily_limit(daily_remaining_request_allowed )
                
        # Per-minute limit handling
        if minute_remaining_request_allowed < 1:
            handle_minute_limit(minute_remaining_request_allowed)

        #raise HTTP error (if status code is 4xx or 5xx)
        response.raise_for_status()

        data = response.json().get("response", [])

        if not data:
            logging.warning(f"No teams found for params: {params}")
            return []
        
        return [{"name": team_data.get("team",{}).get("name",None),
                 "id": team_data.get("team", {}).get("id", None)}
                 for team_data in data
                 if team_data.get("team")]
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error: {e} for endpoint {endpoint}")
    except requests.exceptions.ConnectionError:
        logging.error("Network error: Could not connect to server.")
    except requests.exceptions.Timeout:
        logging.error("Request Timeout")
    except requests.exceptions.RequestException as e:
        logging.error(f"Unexpected error: {e} for endpoint {endpoint}")

    return []

def save_metadata(data, filename="metadata_with_api.json", overwrite=True):
    """
    The function stores the extracted api information and stores it in ./data/metadata.json
    It also has a flag to decide if metadata.json should be overwritten
    """
    base_path = f"./data"
    os.makedirs(base_path, exist_ok=True)

    file_path = os.path.join(base_path, filename)

    if os.path.exists(file_path) and not overwrite:
        logging.info(f"Skipping save: {file_path} already exists.")
        return
    tmp_path = "metadata.tmp.json"
    write_json(tmp_path, data)
    os.replace(tmp_path, file_path)
    logging.info(f"Fixtures saved to: {file_path}")

def extract_metadata():
    
    #params: metadata is a json file of each country, league and season

    metadata = load_json('./data/metadata.json')
    logging.info("Loaded metadata from metadata_with_league_ids.json")

    #list of country with and leagues with missing league id
    missing_league_ids = []
    #league and season of missing teams
    missing_teams =[]

    for country in metadata:
        country_name = country["name"]
        for league in country["leagues"]:
            league_name = league["name"]  

            # Fetch league_id if missing
            if not league.get("league_id"):
                league_id = fetch_leagueid(
                    "leagues",
                    params={"country": country_name, "name": league_name}
                    )
         
                #if league_id is retrieved, store into metadata file 
                if league_id:
                    league["league_id"] = league_id
                    logging.info(f" Got League ID for '{league_name}' in country '{country_name}'")

                #else append country and league name of missing league for debugging
                else:
                    logging.warning(f"League ID not found for '{league_name}' in country'{country_name}'")
                    missing_league_ids.append({"country": country["name"], "league": league["name"]})
                    continue
            
            latest_season = max(season["start_year"] for season in league["seasons"]) 
        
            #Fetch teams if missing
            if not league.get("teams"):
                teams = fetch_teams("teams", params={"league": league["league_id"], "season": latest_season})
                if teams:
                    league["teams"] = teams
                    logging.info(f"Fetched teams for '{league_name}' {latest_season}")
                else :
                    logging.warning(f"No teams for '{league_name}' in {latest_season}")
                    missing_teams.append({"league": league_name, "season": latest_season})
       
    #Report any missing items all at once
    if missing_league_ids:
        logging.error("League missing IDs:\n" + pprint.pformat(missing_league_ids))
    if missing_teams:
        logging.error("League missing teams:\n" + pprint.pformat(missing_teams))

    save_metadata(metadata)
    logging.info("Metadata extraction of league ids and teams of league complete")


if __name__ == "__main__":
    extract_metadata()




    


