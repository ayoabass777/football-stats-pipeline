import etl.src.config as config
import psycopg2
import logging
import json


logging.basicConfig(
    filename= "metadata_log.txt", 
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
    )



DB_CONFIG = config.DB_CONFIG


def test_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info(f"connected to POSTgreSQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
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



# Fetch metadata
metadata = load_metadata('metadata_with_league_ids.json')


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
            logging.info(f"Extracting leagues for {country_id}, {league_name}")
            cursor.execute(
                            """
                            INSERT INTO league (name, country_id)
                            VALUES (%s, %s)
                            ON CONFLICT (name)
                                DO UPDATE
                                    SET name = EXCLUDED.name   -- no real change, just forces RETURNING
                            RETURNING id;
                            """,
                            (league_name, country_id)
                        )
            league_id = cursor.fetchone()[0]
            logging.info(f"Upsert complete: league '{league_name}' has id {league_id}")
            
            #Extracting season from each League's listOf Seasons
            for season in league.get('seasons',[]):
                start_year = season['start_year']
                is_current = season.get('is_current', False)
                cursor.execute("""
                    INSERT INTO season (start_year, is_current)
                    VALUES (%s, %s) 
                    ON CONFLICT (start_year) DO NOTHING;
                    """, (start_year, is_current))
                
                # check whether the INSERT did anything
                if cursor.rowcount:
                    logging.info(f"Inseerted new Season {start_year} with a current_status {is_current}")
                else :
                    logging.info(f"Season '{start_year}' already existed with a current status {is_current}")
              
    
    conn.commit()
    cursor.close()
    conn.close()     

except Exception as e:
    logging.error(f" Error inserting countries from metadata: {e}")