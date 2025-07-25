import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
WEATHERAPI_API_KEY = os.getenv('WEATHERAPI_API_KEY')
WEATHERAPI_BASE_URL = 'http://api.weatherapi.com/v1/current.json'

# Database Configuration
DB_CONFIG = {
    'type': os.getenv('DB_TYPE', 'sqlite'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'weather_db'),
    'username': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Default settings
DEFAULT_CITY = os.getenv('DEFAULT_CITY', 'London')
DATA_OUTPUT_PATH = os.getenv('DATA_OUTPUT_PATH', './data/weather_data.csv')

# Airflow Configuration
AIRFLOW_EMAIL_ON_FAILURE = True
AIRFLOW_EMAIL_ON_RETRY = False
AIRFLOW_RETRIES = 3
AIRFLOW_RETRY_DELAY_MINUTES = 5
