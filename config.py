from dotenv import load_dotenv
import os

#Load the .env file
load_dotenv()

#Retrieve database credentials
DATABASE = {
    'name': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST')
}

#Test if values are loaded
print(DATABASE) 