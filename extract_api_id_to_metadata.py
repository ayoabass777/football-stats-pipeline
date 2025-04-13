import logging
import requests
import json
"""
This file is to extract leageu_id and team_id
from the API. 
Storing the id's in metadata makes it easier, 
direct and consistent to fetch data from API
"""

logging.basicConfig( 
    filename= 'api_to_meta_logs.txt',
    level= logging.DEBUG,
    format= "%(asctime)s - %(levelname)s: %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S"
    )

API_URL = "https://v3.football.api-sports.io/"
HEADERS = {
    'x-rapidapi-key': 'fc243408e4f79d927707f13d0de433a5',
    'x-rapidapi-host': 'v3.football.api-sports.io'
}


def load_json(file_path):
    with open(file_path, 'r') as file:
        json_file = json.load(file)
        logging.info("json file has been loaded into program")
        return json_file
    
def write_json(file_path, data):
    with open(file_path, "w") as file:
        json.dump(data, file, indent= 4)

metadata = load_json('metadata_with_league_ids.json')
logging.info("Loaded metadata from metadata_with_league_ids.json")
logging.info(f"{sum(len(l['teams']) > 0 for c in metadata for l in c['leagues'] if 'teams' in l)} leagues already have teams.")

# endpoint params => league_id
# endpoint is the leagues api endpoint
# params is a dictionary of {"country": "String", "name": "String"}
# country is the name of the country
# name  is the name of the league

#fetch the league_id of a league from a given country
# def fetch_leagueid(leagues, params): pass

def fetch_leagueid(endpoint, params):
    try:
        response = requests.get(f"{API_URL}{endpoint}", headers= HEADERS, params= params)
        
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



#list of country and leagues with missing league_id
missing_league_ids = []

for country in metadata:
    for league in  country["leagues"]:
        if "league_id" in league and league['league_id']:
            continue

        league_id = fetch_leagueid("leagues",
                                    params={"country": country["name"], "name": league["name"]})
        if league_id:
            league["league_id"] = league_id
            logging.info(f"League ID for '{league['name']}' in country '{country['name']}' was uploaded successfully")

        else:
            logging.warning(
                f"League ID not found for '{league['name']}' in country'{country['name']}'"
            )
            missing_league_ids.append({"country": country["name"],
                                       "league": league["name"]
                                       })

# endpoint params => team_name, team_id
# fetch a list of team_name and team_id for the teams in a given league and season 
# where season is_current
# endpoint is the teams api endpoint
# params is a dictionary of {"league": INT, "season": INT}
# league is the league_id
# season is the start_year of the season

def fetch_teams(endpoint, params):
    try:
        response =  requests.get(f"{API_URL}{endpoint}", params= params, headers= HEADERS)
        
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
    
for country in metadata:
    for league in country["leagues"]:
        if "league_id" not in league or not league["league_id"]:
            logging.warning(f"Skipping league without ID: {league['name']}")
            continue
        latest_season = max(season["start_year"] for season in league["seasons"]) 
        
        if "teams" in league and league["teams"]:
            logging.info(f"Skipping league '{league['name']} - teams already fetched")
            continue

        teams = fetch_teams("teams", 
                            params= {"league": league["league_id"], "season": latest_season})
        
        if teams:
            league["teams"] = teams
            logging.info(f"Teams for the current season '{latest_season}' and league '{league['name']}' added successfully")
            write_json("metadata_with_league_ids.json", metadata)
            logging.info(f"api leagues info for league {league} has been dumped into metadata_with_league_ids.json")
        
        else:
            logging.warning(f"teams for league '{league['name']}' in season {latest_season} not found")
    


##handle rate limits


    


