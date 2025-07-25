"""
Test cases for weather data extraction
"""
import pytest
import requests_mock
from scripts.extract_weather import extract_weather_data
from config.config import WEATHERAPI_BASE_URL


class TestWeatherExtraction:
    """Test cases for weather data extraction"""
    
    def test_extract_weather_data_success(self, sample_weather_api_response, mock_task_instance):
        """Test successful weather data extraction"""
        with requests_mock.Mocker() as m:
            m.get(WEATHERAPI_BASE_URL, json=sample_weather_api_response)
            
            context = {'task_instance': mock_task_instance}
            result = extract_weather_data(city="London", **context)
            
            assert result is not None
            assert 'location' in result
            assert 'current' in result
            assert result['location']['name'] == 'London'
            assert result['current']['temp_c'] == 15.5
    
    def test_extract_weather_data_api_error(self, mock_task_instance):
        """Test weather data extraction with API error"""
        with requests_mock.Mocker() as m:
            m.get(WEATHERAPI_BASE_URL, status_code=401)
            
            context = {'task_instance': mock_task_instance}
            
            with pytest.raises(Exception):
                extract_weather_data(city="London", **context)
    
    def test_extract_weather_data_invalid_city(self, mock_task_instance):
        """Test weather data extraction with invalid city"""
        with requests_mock.Mocker() as m:
            m.get(WEATHERAPI_BASE_URL, status_code=400, json={'error': {'message': 'No matching location found'}})
            
            context = {'task_instance': mock_task_instance}
            
            with pytest.raises(Exception):
                extract_weather_data(city="InvalidCity123", **context)
