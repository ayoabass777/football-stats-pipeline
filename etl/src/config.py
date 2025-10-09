from dotenv import load_dotenv
import os

#Load the .env file
load_dotenv(override=True)


DB_CONFIG = {
        "dbname": os.getenv("DB_NAME"),
        "user":  os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        'port': int(os.getenv('DB_PORT')),
    }



#Rapid Api
API = {
    'key': os.getenv('API_KEY'),
    'host': os.getenv('API_HOST'),
    'url': os.getenv('API_URL')
}

MIN_SEASON_TRACKER = int(os.getenv('MIN_SEASON_TRACKER', '2020'))



# File paths
BASE_DATA_DIR = os.getenv("BASE_DATA_DIR")
LOGS_PATH = os.getenv("LOGS_PATH")
FIXTURES_PATH = os.getenv("FIXTURES_PATH")
FIXTURES_UPDATE_DIR = os.getenv("FIXTURES_UPDATE_DIR")
CLEANED_DATA_DIR = os.getenv("CLEANED_DATA_DIR")

# Ensure required data directories exist
if LOGS_PATH:
    os.makedirs(LOGS_PATH, exist_ok=True)

if FIXTURES_PATH:
    os.makedirs(FIXTURES_PATH, exist_ok=True)

if FIXTURES_UPDATE_DIR:
    os.makedirs(FIXTURES_UPDATE_DIR, exist_ok=True)

if CLEANED_DATA_DIR:
    os.makedirs(CLEANED_DATA_DIR, exist_ok=True)

# Specific log file paths
EXTRACT_METADATA_LOG = os.getenv(
    "EXTRACT_METADATA_LOG",
    os.path.join(LOGS_PATH, "extract_metadata_logs.txt")
)
EXTRACT_FIXTURES_LOG = os.getenv(
    "EXTRACT_FIXTURES_LOG",
    os.path.join(LOGS_PATH, "extract_fixtures_logs.txt")
)

TRANSFORM_FIXTURES_LOG = os.getenv(
    "TRANSFORM_FIXTURES_LOG",
    os.path.join(LOGS_PATH, "transforming_fixtures_logs.txt")
)


# Log file path for loading raw fixtures into the database
LOAD_TO_DB_LOG = os.getenv(
    "LOAD_TO_DB_LOG",
    os.path.join(LOGS_PATH, "load_to_db_logs.txt")
)


# Log file path for updating played fixtures
UPDATE_FIXTURES_LOG = os.getenv(
    "UPDATE_FIXTURES_LOG",
    os.path.join(LOGS_PATH, "update_fixtures_log.txt")
)

# Path to JSON file containing fixture updates (played or postponed)
FIXTURE_UPDATES_JSON = os.getenv(
    "FIXTURE_UPDATES_JSON",
    os.path.join(FIXTURES_PATH, "fixture_updates.json")
)

LOAD_UPDATES_LOG = os.getenv(
    "LOAD_UPDATES_LOG",
    os.path.join(LOGS_PATH, "load_updates_to_db_logs.txt")
)

# Metadata YAML file path
METADATA_FILE = os.getenv(
    "METADATA_FILE",
    os.path.join(os.getcwd(), "data", "metadata.yaml")
)