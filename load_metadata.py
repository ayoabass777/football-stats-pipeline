import sys
import os 
import psycopg2
import requests
import logging
import json

logging.basicConfig(
    filename= "log.txt", 
    level=logging.DEBUG, 
    format="%(asctime)s - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
    )

#API Configuration
API_URL = "https://v3.football.api-sports.io/"
HEADERS = {
    'x-rapidapi-key': 'fc243408e4f79d927707f13d0de433a5',
    'x-rapidapi-host': 'v3.football.api-sports.io'
}

#Database Configuration for Docker
DB_CONFIG = {
    "dbname" : "football_betting",
    "user": "ayoabass",
    "password": "tamzynana",
    "host": "localhost",
    "port": "5433"
}

def test_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logging.debug(f"connected to POSTgreSQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        conn.close()
    except Exception as e:
        logging.error(f"Test Connection failed: {e}")
test_db_connection()

""" 
-Metadata is Countries =>  List of country
- A country is country with keys made of name and Leagues => ListOf league"""



#Loading metadata
def load_metadata(file_path):
    with open(file_path, "r") as  file:
        metadata = json.load(file)
        return metadata



# Fetch Countries
metadata = load_metadata('metadata.json')


#Connect to database and insert countries, league and seasons
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # extracting country in metadata- listOf Country
    for country in metadata:
        country_name = country['name']
        cursor.execute(                 
        """
        INSERT INTO country (name)
        VALUES (%s)
        ON CONFLICT (name) DO NOTHING
        RETURNING id;
        """, (country_name,))
        result = cursor.fetchone()
        if result:
            country_id = result[0]
            logging.info(f"Inserted country '{country_name}' with id {country_id}.")
        else:
            cursor.execute("""
                SELECT id 
                FROM country 
                WHERE name = %s;
                """, (country_name,))
            country_id =cursor.fetchone()[0]
            logging.info(f"Country {country_name} already exists with id {country_id}")
        
        #Extracting League in each country's listOf League
        for league in country.get("leagues", []):
            league_name = league['name']
            cursor.execute(
                """
                INSERT INTO league (name, country_id)
                VALUES (%s, %s)
                ON CONFLICT(name) DO NOTHING
                RETURNING id;
                """, (league_name, country_id))
            result = cursor.fetchone()[0]
            if result:
                league_id = result
                logging.info(f"Inserted league '{league_name}' with id {league_id}")
            else:
                cursor.execute("""
                    SELECT id 
                    FROM league
                    WHERE name = %s
                    """, (league_name))
                league_id = cursor.fetchone()[0]
                logging.info(f"League {league_name} alread exists with id {league_id}")
            
            #Extracting season from each League's listOf Seasons
            for season in league.get('seasons',[]):
                start_year = season['start_year']
                is_current = season.get('is_current', False)
                cursor.execute("""
                    INSERT INTO season (start_year, is_current)
                    VALUES (%s, %s) 
                    ON CONFLICT (start_year) DO UPDATE SET is_current = EXCLUDED.is_current
                    RETURNING id""", (start_year, is_current))
                result = cursor.fetchone()[0]
                if result:
                    season_id = result
                    logging.info(f"Inserted Season: {start_year}, current_status: {is_current}, id: {season_id}")
                else:
                    cursor.execute("""SELECT id, is_current
                                   FROM season
                                   WHERE start_year = %s
                                   """, (start_year,))
    
    conn.commit()
    cursor.close()
    conn.close()     

except Exception as e:
    logging.error(f" Error inserting countries from metadata: {e}")