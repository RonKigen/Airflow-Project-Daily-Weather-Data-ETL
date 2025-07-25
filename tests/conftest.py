"""
Pytest configuration and fixtures for Weather ETL tests
"""
import pytest
import sys
import os
from datetime import datetime

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

@pytest.fixture
def sample_weather_api_response():
    """Sample WeatherAPI.com response for testing"""
    return {
        'location': {
            'name': 'London',
            'country': 'United Kingdom'
        },
        'current': {
            'temp_c': 15.5,
            'feelslike_c': 14.2,
            'humidity': 78,
            'pressure_mb': 1013.2,
            'wind_kph': 11.5,
            'wind_degree': 250,
            'condition': {'text': 'Scattered Clouds'},
            'vis_km': 10.0
        },
        'extraction_timestamp': datetime.utcnow().isoformat()
    }

@pytest.fixture
def sample_transformed_data():
    """Sample transformed weather data for testing"""
    return {
        'timestamp': datetime.utcnow(),
        'city': 'London',
        'country': 'United Kingdom',
        'temperature': 15.5,
        'feels_like': 14.2,
        'humidity': 78,
        'pressure': 1013.2,
        'wind_speed': 3.2,
        'wind_direction': 250,
        'weather_main': 'Scattered Clouds',
        'weather_description': 'Scattered Clouds',
        'visibility': 10000.0,
        'sunrise': None,
        'sunset': None,
        'extraction_timestamp': datetime.utcnow().isoformat()
    }

@pytest.fixture
def mock_task_instance():
    """Mock Airflow TaskInstance for testing"""
    class MockTaskInstance:
        def __init__(self):
            self.data = {}
        
        def xcom_push(self, key, value):
            self.data[key] = value
        
        def xcom_pull(self, key):
            return self.data.get(key)
    
    return MockTaskInstance()
