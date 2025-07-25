"""
Test cases for weather data transformation
"""
import pytest
from datetime import datetime
from scripts.transform_weather import transform_weather_data, clean_weather_data


class TestWeatherTransformation:
    """Test cases for weather data transformation"""
    
    def test_transform_weather_data_success(self, sample_weather_api_response, mock_task_instance):
        """Test successful weather data transformation"""
        context = {'task_instance': mock_task_instance}
        
        result = transform_weather_data(raw_data=sample_weather_api_response, **context)
        
        assert result is not None
        assert 'city' in result
        assert 'temperature' in result
        assert 'humidity' in result
        assert result['city'] == 'London'
        assert result['temperature'] == 15.5
        assert result['humidity'] == 78
        assert result['wind_speed'] == pytest.approx(3.19, rel=1e-1)  # 11.5 kph to m/s
    
    def test_clean_weather_data_temperature_validation(self):
        """Test temperature validation in data cleaning"""
        # Valid temperature
        data = {'temperature': 25.0, 'humidity': 50, 'wind_speed': 5.0, 'pressure': 1013}
        cleaned = clean_weather_data(data)
        assert cleaned['temperature'] == 25.0
        
        # Extreme temperature (should log warning but not change value)
        data = {'temperature': -100.0, 'humidity': 50, 'wind_speed': 5.0, 'pressure': 1013}
        cleaned = clean_weather_data(data)
        assert cleaned['temperature'] == -100.0  # Value preserved, but warning logged
    
    def test_clean_weather_data_humidity_validation(self):
        """Test humidity validation in data cleaning"""
        # Valid humidity
        data = {'temperature': 25.0, 'humidity': 50, 'wind_speed': 5.0, 'pressure': 1013}
        cleaned = clean_weather_data(data)
        assert cleaned['humidity'] == 50
        
        # Invalid humidity (should be clamped)
        data = {'temperature': 25.0, 'humidity': 150, 'wind_speed': 5.0, 'pressure': 1013}
        cleaned = clean_weather_data(data)
        assert cleaned['humidity'] == 100
        
        data = {'temperature': 25.0, 'humidity': -10, 'wind_speed': 5.0, 'pressure': 1013}
        cleaned = clean_weather_data(data)
        assert cleaned['humidity'] == 0
    
    def test_clean_weather_data_wind_speed_validation(self):
        """Test wind speed validation in data cleaning"""
        # Valid wind speed
        data = {'temperature': 25.0, 'humidity': 50, 'wind_speed': 5.0, 'pressure': 1013}
        cleaned = clean_weather_data(data)
        assert cleaned['wind_speed'] == 5.0
        
        # Negative wind speed (should be set to 0)
        data = {'temperature': 25.0, 'humidity': 50, 'wind_speed': -5.0, 'pressure': 1013}
        cleaned = clean_weather_data(data)
        assert cleaned['wind_speed'] == 0
    
    def test_transform_missing_data(self, mock_task_instance):
        """Test transformation with missing data fields"""
        incomplete_data = {
            'location': {'name': 'TestCity'},
            'current': {'temp_c': 20.0}
            # Missing many fields
        }
        
        context = {'task_instance': mock_task_instance}
        result = transform_weather_data(raw_data=incomplete_data, **context)
        
        assert result is not None
        assert result['city'] == 'TestCity'
        assert result['temperature'] == 20.0
        assert result['humidity'] == 0  # Default value
        assert result['pressure'] == 0   # Default value
