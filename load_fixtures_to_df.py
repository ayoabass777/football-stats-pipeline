import pandas as pd
import glob
import logging
from pathlib import Path
from sqlalchemy import create_engine

logging.basicConfig(
    filename= 'load_fixtures_to_df_logs.txt',
    level= logging.INFO,
    format= "%(asctime)s - %(levelname)s :%(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S"
)

def get_second_half_score(row):
    """Takes in a df and create a second half score based on
    first half score and full time score
    """
    pass

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


df_fixtures = load_all_fixtures()

#Check for duplicate fixture IDs
duplicate_rows = df_fixtures.duplicated(subset=["fixture_id"], keep= False)

if not duplicate_rows.empty:
    logging.warning(f"Found {len(duplicate_rows)} duplicate fixture(s)")
    df_fixtures.drop_duplicates(subset=["fixture_id"], inplace=True)
    logging.info(f"All duplicates have been removed successfully")
else:
    logging.info("No duplicate fixture IDs detected")

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

#Apply to dataframe
df_fixtures[["home_fulltime_result", "away_fulltime_result"]] = df_fixtures.apply(
    get_fulltime_home_away_results, axis=1)

df_fixtures[["home_halftime_result", "away_halftime_result"]] = df_fixtures.apply(
    get_halftime_home_away_results, axis=1
)

#Convert from float64 to Int64
df_fixtures["home_team_fulltime_goal"] = df_fixtures["home_team_fulltime_goal"].astype("Int64")
df_fixtures["away_team_fulltime_goal"] = df_fixtures["away_team_fulltime_goal"].astype("Int64")

df_fixtures["home_team_halftime_goal"] = df_fixtures["home_team_halftime_goal"].astype("Int64")
df_fixtures["away_team_halftime_goal"] = df_fixtures["away_team_halftime_goal"].astype("Int64")

df_fixtures.to_csv("cleaned_fixtures.csv", index= False)
logging.info("Cleaned_fixtures csv file has been stored")

df_fixtures.to_parquet("cleaned_fixtures.parquet", index= False)
logging.info("Cleaned_fixtures csv file has been stored")


def load_to_db (df, table_name="raw_fixtures", if_exists="replace"):
    """
    Loads a DataFrame into a PostgreSQL table

    Paramters:
        df (DataFrame): The pandas DataFrame to load.
        table_name (str): The name of the target table in the database.
        if_exists (str): 'replace', 'append', or 'fail'
    """
    engine = create_engine('postgresql+psycopg2://ayoabass:tamzynana@localhost:5433/football_betting')
    
    try:
        df.to_sql(table_name, engine, if_exists= if_exists, index=False)
        logging.info(f"Loaded {len(df)} rows into {table_name} successfully")

        #first 10 rows in fixtures
        fixtures_10 = pd.read_sql("select * FROM raw_fixtures LIMIT 10", engine )
        print(fixtures_10)
    
    except Exception as e:
        logging.error(f"Failed to load data into table '{table_name}': {e}")


load_to_db(df_fixtures)