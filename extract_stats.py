import logging
import requests

#API Configuration
API_URL = "https://v3.football.api-sports.io/"
HEADERS = {
    'x-rapidapi-key': 'fc243408e4f79d927707f13d0de433a5',
    'x-rapidapi-host': 'v3.football.api-sports.io'
}

# endpoint, params -> json file
# get the API json file from the given endpoin
def fetch_data(endpoint, params=None):
    '''Function to fetch data from API'''
    try:    
        response = requests.get(f"{API_URL}{endpoint}", headers= HEADERS, params=params)

        #Raise an HTTP error (if status_code is 4xx or 5xx)
        response.raise_for_status()
        return response.json()["response"]
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error: {e}")
    except requests.exceptions.ConnectionError:
        logging.error("Network error: Could not connect to the server.")
    except requests.exceptions.Timeout:
        logging.error("Request timed out")
    except requests.exceptions.RequestException as e:
        logging.error(f"Unexpected error: {e} for endpoint {endpoint}")
    
    # if any error occurs
    return None 