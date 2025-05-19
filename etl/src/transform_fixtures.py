import pandas as pd
import glob
import logging
from pathlib import Path
from sqlalchemy import create_engine

logging.basicConfig(
    filename= './data/logs/transforming_fixtures_logs.txt',
    level= logging.INFO,
    format= "%(asctime)s - %(levelname)s :%(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S"
)

def load_all_fixtures(base_dir="data/fixtures"):
    """
    This function takes in the base directory of where fixtures
    of different leagues from different countries are stored
    and produces a dataframe of all the fixtures concatenated 
    """
    #Hold the fixtures from different leagues as iterables
    all_fixtures = []

    #Find all fixtures.json files in subfolders
    json_file_paths = glob.glob(f"{base_dir}/**/fixtures.json", recursive=True)

    for file_path in json_file_paths:
        try:
            # Extract country, league, and season from the folder path
            parts = Path(file_path).parts
            
            # load fixture json to pandas 
            df = pd.read_json(file_path)

            # Example: ['data', 'fixtures', 'spain', 'la_liga', '2024', 'fixtures.json']
            df['country'] = parts[2]
            df['league'] = parts[3]
            df['season'] = parts[4]

            all_fixtures.append(df)
            logging.info(f"Loaded {file_path} with {len(df)} fixtures")

        except Exception as e:
            logging.warning(f"Failed to load {file_path}: {e}")

    
    if not all_fixtures:
        logging.warning("No fixture files found or all failed to load")
        return pd.DataFrame()
    
    return pd.concat(all_fixtures, ignore_index= True)

#get home and away results
def get_home_away_results(home_score, away_score):
    """
    Takes in series of of two teams scores and produce the match result
    """ 
    if pd.isna(home_score) or pd.isna(away_score):
        return pd.Series([None, None])
    
    if home_score > away_score:
        return pd.Series(["win","loss"])
    
    elif home_score < away_score:
        return pd.Series(["loss", "win"])
    
    return pd.Series(["draw", "draw"])


#get fulltime home and away results row by row
def get_fulltime_home_away_results(row):
    home_score = row["home_team_fulltime_goal"]
    away_score = row["away_team_fulltime_goal"]
    return get_home_away_results(home_score, away_score)


#get halftime home and away results row by row
def get_halftime_home_away_results(row):
    home_score = row["home_team_halftime_goal"]
    away_score = row["away_team_halftime_goal"]
    return get_home_away_results(home_score, away_score)




def transform_fixtures():

    #Calls the fixture loader function
    df_fixtures = load_all_fixtures() #def kwargs is basedir= "data/fixtures")

    logging.info(f"A total of {len(df_fixtures)} fixtures were added")

    #converts date to pd to datetime object
    df_fixtures['date'] = pd.to_datetime(df_fixtures["date"])
    logging.info("Date has been converted to pd.to_datetime object")

    #Apply to dataframe
    df_fixtures[["home_fulltime_result", "away_fulltime_result"]] = df_fixtures.apply(
        get_fulltime_home_away_results, axis=1
        )

    df_fixtures[["home_halftime_result", "away_halftime_result"]] = df_fixtures.apply(
        get_halftime_home_away_results, axis=1
        )

#Convert from float64 to Int64
    df_fixtures["home_team_fulltime_goal"] = df_fixtures["home_team_fulltime_goal"].astype("Int64")
    df_fixtures["away_team_fulltime_goal"] = df_fixtures["away_team_fulltime_goal"].astype("Int64")

    df_fixtures["home_team_halftime_goal"] = df_fixtures["home_team_halftime_goal"].astype("Int64")
    df_fixtures["away_team_halftime_goal"] = df_fixtures["away_team_halftime_goal"].astype("Int64")

    df_fixtures.to_csv("./data/cleaned_fixtures.csv", index= False)
    logging.info("Cleaned_fixtures csv file has been stored for analysis")

    df_fixtures.to_parquet("./data/cleaned_fixtures.parquet", index= False)
    logging.info("Cleaned_fixtures parquet file has been stored and will be sent to database")


if __name__ == "__main__":
    transform_fixtures()
