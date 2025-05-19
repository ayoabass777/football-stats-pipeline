from dotenv import load_dotenv
import os

#Load the .env file
load_dotenv(override=True)


DB_CONFIG = {
        "dbname": os.getenv("DB_NAME"),
        "user":  os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        'port': int(os.getenv('DB_PORT', 5433)),
    }



#Rapid Api
API = {
    'key': os.getenv('API_KEY'),
    'host': os.getenv('API_HOST'),
    'url': os.getenv('API_URL')
}