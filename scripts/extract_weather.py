import requests
import json
import logging
from datetime import datetime
from config.config import WEATHERAPI_API_KEY, WEATHERAPI_BASE_URL, DEFAULT_CITY

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_weather_data(city=None, **context):
    """
    Extract weather data from WeatherAPI.com
    
    Args:
        city (str): City name to get weather data for
        **context: Airflow context
        
    Returns:
        dict: Raw weather data from API
    """
    try:
        city = city or DEFAULT_CITY
        
        if not WEATHERAPI_API_KEY:
            raise ValueError("WeatherAPI key not found. Please set WEATHERAPI_API_KEY in environment variables.")
        
        # Prepare API request parameters
        params = {
            'key': WEATHERAPI_API_KEY,
            'q': city,
            'aqi': 'no'  # Air quality data not needed for basic weather
        }
        
        logger.info(f"Extracting weather data for city: {city}")
        
        # Make API request
        response = requests.get(WEATHERAPI_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        
        weather_data = response.json()
        
        # Add extraction timestamp
        weather_data['extraction_timestamp'] = datetime.utcnow().isoformat()
        
        logger.info(f"Successfully extracted weather data for {city}")
        logger.info(f"Temperature: {weather_data['current']['temp_c']}°C")
        logger.info(f"Humidity: {weather_data['current']['humidity']}%")
        
        # Store data in XCom for next task
        context['task_instance'].xcom_push(key='raw_weather_data', value=weather_data)
        
        return weather_data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request: {str(e)}")
        raise
    except KeyError as e:
        logger.error(f"Unexpected API response format: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during extraction: {str(e)}")
        raise

def extract_multiple_cities(cities=None, **context):
    """
    Extract weather data for multiple cities
    
    Args:
        cities (list): List of city names
        **context: Airflow context
        
    Returns:
        list: List of weather data for all cities
    """
    if not cities:
        cities = [DEFAULT_CITY]
    
    all_weather_data = []
    
    for city in cities:
        try:
            weather_data = extract_weather_data(city)
            all_weather_data.append(weather_data)
        except Exception as e:
            logger.error(f"Failed to extract data for {city}: {str(e)}")
            # Continue with other cities
            continue
    
    context['task_instance'].xcom_push(key='all_weather_data', value=all_weather_data)
    return all_weather_data

if __name__ == "__main__":
    # Test the extraction function
    test_data = extract_weather_data("London")
    print(json.dumps(test_data, indent=2))
