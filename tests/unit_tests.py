#!/usr/bin/env python3
"""
Simple test runner for Weather ETL Pipeline
Alternative to pytest for basic testing
"""

import sys
import os
import unittest
from datetime import datetime

# Add project paths
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class TestWeatherETL(unittest.TestCase):
    """Basic unit tests for Weather ETL Pipeline"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.sample_api_response = {
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
        
        class MockTaskInstance:
            def __init__(self):
                self.data = {}
            def xcom_push(self, key, value):
                self.data[key] = value
            def xcom_pull(self, key):
                return self.data.get(key)
        
        self.mock_task_instance = MockTaskInstance()
    
    def test_config_loading(self):
        """Test configuration loading"""
        try:
            from config.config import WEATHERAPI_API_KEY, DEFAULT_CITY, DB_CONFIG
            self.assertIsNotNone(DEFAULT_CITY)
            self.assertIsNotNone(DB_CONFIG)
            self.assertIn('type', DB_CONFIG)
            print("✅ Configuration loading test passed")
        except ImportError as e:
            self.fail(f"Failed to import config: {e}")
    
    def test_transformation(self):
        """Test weather data transformation"""
        try:
            from scripts.transform_weather import transform_weather_data
            
            context = {'task_instance': self.mock_task_instance}
            result = transform_weather_data(raw_data=self.sample_api_response, **context)
            
            self.assertIsNotNone(result)
            self.assertIn('city', result)
            self.assertIn('temperature', result)
            self.assertIn('humidity', result)
            self.assertEqual(result['city'], 'London')
            self.assertEqual(result['temperature'], 15.5)
            self.assertEqual(result['humidity'], 78)
            print("✅ Weather data transformation test passed")
            
        except Exception as e:
            self.fail(f"Transformation test failed: {e}")
    
    def test_data_cleaning(self):
        """Test data cleaning functions"""
        try:
            from scripts.transform_weather import clean_weather_data
            
            # Test valid data
            data = {
                'temperature': 25.0,
                'humidity': 50,
                'wind_speed': 5.0,
                'pressure': 1013,
                'city': ' london ',
                'weather_description': ' partly cloudy '
            }
            
            cleaned = clean_weather_data(data)
            
            self.assertEqual(cleaned['humidity'], 50)
            self.assertEqual(cleaned['wind_speed'], 5.0)
            self.assertEqual(cleaned['city'], 'London')  # Should be cleaned
            self.assertEqual(cleaned['weather_description'], 'Partly Cloudy')  # Should be cleaned
            
            # Test invalid humidity (should be clamped)
            data['humidity'] = 150
            cleaned = clean_weather_data(data)
            self.assertEqual(cleaned['humidity'], 100)
            
            data['humidity'] = -10
            cleaned = clean_weather_data(data)
            self.assertEqual(cleaned['humidity'], 0)
            
            # Test negative wind speed (should be set to 0)
            data['wind_speed'] = -5.0
            cleaned = clean_weather_data(data)
            self.assertEqual(cleaned['wind_speed'], 0)
            
            print("✅ Data cleaning test passed")
            
        except Exception as e:
            self.fail(f"Data cleaning test failed: {e}")
    
    def test_csv_creation(self):
        """Test CSV file operations"""
        try:
            import tempfile
            from scripts.load_weather import load_to_csv
            
            sample_data = {
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
            
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
                temp_path = temp_file.name
            
            try:
                # Temporarily override the output path
                import config.config as config_module
                original_path = config_module.DATA_OUTPUT_PATH
                config_module.DATA_OUTPUT_PATH = temp_path
                
                load_to_csv(sample_data)
                
                # Verify file exists and contains data
                self.assertTrue(os.path.exists(temp_path))
                
                with open(temp_path, 'r') as f:
                    content = f.read()
                    self.assertIn('London', content)
                    self.assertIn('15.5', content)
                
                print("✅ CSV creation test passed")
                
            finally:
                # Cleanup
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                # Restore original config
                config_module.DATA_OUTPUT_PATH = original_path
                    
        except Exception as e:
            self.fail(f"CSV creation test failed: {e}")


def main():
    """Run the test suite"""
    print("🧪 Weather ETL Unit Test Suite")
    print("=" * 50)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestWeatherETL)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 50)
    if result.wasSuccessful():
        print("🎉 All unit tests passed!")
    else:
        print("❌ Some tests failed!")
        print(f"Failures: {len(result.failures)}")
        print(f"Errors: {len(result.errors)}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
