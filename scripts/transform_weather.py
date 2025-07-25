import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_weather_data(raw_data=None, **context):
    """
    Transform raw weather data into clean, structured format
    
    Args:
        raw_data (dict): Raw weather data from API
        **context: Airflow context
        
    Returns:
        dict: Transformed weather data
    """
    try:
        # Get data from XCom if not provided directly
        if raw_data is None:
            raw_data = context['task_instance'].xcom_pull(key='raw_weather_data')
        
        if not raw_data:
            raise ValueError("No weather data found to transform")
        
        logger.info("Starting weather data transformation")
        
        # Extract relevant fields from the raw data (WeatherAPI.com format)
        transformed_data = {
            'timestamp': datetime.utcnow(),
            'city': raw_data.get('location', {}).get('name', 'Unknown'),
            'country': raw_data.get('location', {}).get('country', 'Unknown'),
            'temperature': round(raw_data.get('current', {}).get('temp_c', 0), 2),
            'feels_like': round(raw_data.get('current', {}).get('feelslike_c', 0), 2),
            'humidity': raw_data.get('current', {}).get('humidity', 0),
            'pressure': raw_data.get('current', {}).get('pressure_mb', 0),
            'wind_speed': round(raw_data.get('current', {}).get('wind_kph', 0) * 0.277778, 2),  # Convert kph to m/s
            'wind_direction': raw_data.get('current', {}).get('wind_degree', 0),
            'weather_main': raw_data.get('current', {}).get('condition', {}).get('text', 'Unknown'),
            'weather_description': raw_data.get('current', {}).get('condition', {}).get('text', 'Unknown'),
            'visibility': raw_data.get('current', {}).get('vis_km', 0) * 1000,  # Convert km to meters
            'sunrise': None,  # WeatherAPI.com doesn't provide sunrise/sunset in current weather
            'sunset': None,
            'extraction_timestamp': raw_data.get('extraction_timestamp')
        }
        
        # Data quality checks and cleaning
        transformed_data = clean_weather_data(transformed_data)
        
        logger.info(f"Successfully transformed weather data for {transformed_data['city']}")
        logger.info(f"Cleaned data: Temp={transformed_data['temperature']}°C, Humidity={transformed_data['humidity']}%")
        
        # Store transformed data in XCom
        context['task_instance'].xcom_push(key='transformed_weather_data', value=transformed_data)
        
        return transformed_data
        
    except Exception as e:
        logger.error(f"Error during data transformation: {str(e)}")
        raise

def clean_weather_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean and validate weather data
    
    Args:
        data (dict): Raw transformed data
        
    Returns:
        dict: Cleaned data
    """
    cleaned_data = data.copy()
    
    # Temperature validation (reasonable range: -100 to 60 Celsius)
    if not (-100 <= cleaned_data['temperature'] <= 60):
        logger.warning(f"Unusual temperature value: {cleaned_data['temperature']}°C")
    
    # Humidity validation (0-100%)
    if not (0 <= cleaned_data['humidity'] <= 100):
        logger.warning(f"Invalid humidity value: {cleaned_data['humidity']}%")
        cleaned_data['humidity'] = max(0, min(100, cleaned_data['humidity']))
    
    # Wind speed validation (non-negative)
    if cleaned_data['wind_speed'] < 0:
        logger.warning(f"Negative wind speed: {cleaned_data['wind_speed']} m/s")
        cleaned_data['wind_speed'] = 0
    
    # Pressure validation (reasonable range: 870-1085 hPa)
    if not (870 <= cleaned_data['pressure'] <= 1085):
        logger.warning(f"Unusual pressure value: {cleaned_data['pressure']} hPa")
    
    # Clean text fields
    cleaned_data['city'] = cleaned_data['city'].strip().title()
    cleaned_data['weather_description'] = cleaned_data['weather_description'].strip().title()
    
    return cleaned_data

def transform_multiple_weather_data(**context):
    """
    Transform weather data for multiple cities
    
    Args:
        **context: Airflow context
        
    Returns:
        list: List of transformed weather data
    """
    try:
        all_raw_data = context['task_instance'].xcom_pull(key='all_weather_data')
        
        if not all_raw_data:
            raise ValueError("No weather data found to transform")
        
        all_transformed_data = []
        
        for raw_data in all_raw_data:
            try:
                transformed_data = transform_weather_data(raw_data)
                all_transformed_data.append(transformed_data)
            except Exception as e:
                logger.error(f"Failed to transform data for {raw_data.get('name', 'Unknown')}: {str(e)}")
                continue
        
        context['task_instance'].xcom_push(key='all_transformed_data', value=all_transformed_data)
        return all_transformed_data
        
    except Exception as e:
        logger.error(f"Error during multiple data transformation: {str(e)}")
        raise

def create_weather_dataframe(transformed_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Create a pandas DataFrame from transformed weather data
    
    Args:
        transformed_data (list): List of transformed weather dictionaries
        
    Returns:
        pd.DataFrame: Weather data as DataFrame
    """
    if isinstance(transformed_data, dict):
        transformed_data = [transformed_data]
    
    df = pd.DataFrame(transformed_data)
    
    # Convert timestamp columns to datetime
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    if 'sunrise' in df.columns:
        df['sunrise'] = pd.to_datetime(df['sunrise'])
    if 'sunset' in df.columns:
        df['sunset'] = pd.to_datetime(df['sunset'])
    
    return df

if __name__ == "__main__":
    # Test transformation with sample data (WeatherAPI.com format)
    sample_data = {
        'location': {'name': 'London', 'country': 'United Kingdom'},
        'current': {
            'temp_c': 15.5,
            'feelslike_c': 14.2,
            'humidity': 78,
            'pressure_mb': 1013.2,
            'wind_kph': 11.5,  # Will be converted to m/s
            'wind_degree': 250,
            'condition': {'text': 'Scattered Clouds'},
            'vis_km': 10.0  # Will be converted to meters
        },
        'extraction_timestamp': datetime.utcnow().isoformat()
    }
    
    result = transform_weather_data(sample_data)
    print(f"Transformed data: {result}")
