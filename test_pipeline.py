#!/usr/bin/env python3
"""
Test script for Weather ETL Pipeline
Run this script to test individual components without Airflow
"""

import sys
import os
import json
from datetime import datetime

# Add project paths
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))
sys.path.append(os.path.dirname(__file__))  # Add root directory for config module

def test_extract():
    """Test weather data extraction"""
    print("🔍 Testing weather data extraction...")
    
    try:
        from scripts.extract_weather import extract_weather_data
        
        # Mock context for testing
        class MockTaskInstance:
            def xcom_push(self, key, value):
                print(f"XCom Push - {key}: {type(value).__name__}")
        
        class MockContext:
            def __init__(self):
                self.task_instance = MockTaskInstance()
            def __getitem__(self, key):
                if key == 'task_instance':
                    return self.task_instance
        
        context = MockContext()
        result = extract_weather_data(city="London", **{'task_instance': context.task_instance})
        
        if result and 'current' in result:
            print("✅ Extraction test passed!")
            print(f"   Temperature: {result['current']['temp_c']}°C")
            print(f"   City: {result['location']['name']}")
            return result
        else:
            print("❌ Extraction test failed - Invalid response format")
            return None
            
    except Exception as e:
        print(f"❌ Extraction test failed: {str(e)}")
        if "API key" in str(e):
            print("   💡 Make sure to set your WeatherAPI key in .env file")
        return None

def test_transform():
    """Test weather data transformation"""
    print("\n🔄 Testing weather data transformation...")
    
    try:
        from scripts.transform_weather import transform_weather_data
        
        # Mock context for testing
        class MockTaskInstance:
            def xcom_push(self, key, value):
                print(f"XCom Push - {key}: {type(value).__name__}")
            
            def xcom_pull(self, key):
                return None
        
        # Sample data for testing (WeatherAPI.com format)
        sample_data = {
            'location': {'name': 'London', 'country': 'United Kingdom'},
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
        
        result = transform_weather_data(sample_data, **{'task_instance': MockTaskInstance()})
        
        if result and 'temperature' in result and 'humidity' in result:
            print("✅ Transformation test passed!")
            print(f"   Cleaned Temperature: {result['temperature']}°C")
            print(f"   Cleaned Humidity: {result['humidity']}%")
            print(f"   Weather Description: {result['weather_description']}")
            return result
        else:
            print("❌ Transformation test failed - Invalid output format")
            return None
            
    except Exception as e:
        print(f"❌ Transformation test failed: {str(e)}")
        return None

def test_load():
    """Test weather data loading"""
    print("\n💾 Testing weather data loading...")
    
    try:
        from scripts.load_weather import load_to_csv, load_to_database
        
        # Mock context for testing
        class MockTaskInstance:
            def xcom_push(self, key, value):
                pass
            
            def xcom_pull(self, key):
                return {
                    'timestamp': datetime.utcnow(),
                    'city': 'London',
                    'country': 'GB',
                    'temperature': 15.5,
                    'feels_like': 14.2,
                    'humidity': 78,
                    'pressure': 1013.2,
                    'wind_speed': 3.2,
                    'wind_direction': 250,
                    'weather_main': 'Clouds',
                    'weather_description': 'Scattered Clouds',
                    'visibility': 10000,
                    'sunrise': datetime.utcnow(),
                    'sunset': datetime.utcnow(),
                    'extraction_timestamp': datetime.utcnow().isoformat()
                }
        
        # Sample transformed data
        sample_data = {
            'timestamp': datetime.utcnow(),
            'city': 'London',
            'country': 'GB',
            'temperature': 15.5,
            'feels_like': 14.2,
            'humidity': 78,
            'pressure': 1013.2,
            'wind_speed': 3.2,
            'wind_direction': 250,
            'weather_main': 'Clouds',
            'weather_description': 'Scattered Clouds',
            'visibility': 10000,
            'sunrise': datetime.utcnow(),
            'sunset': datetime.utcnow(),
            'extraction_timestamp': datetime.utcnow().isoformat()
        }
        
        # Test CSV loading
        load_to_csv(sample_data)
        print("✅ CSV loading test passed!")
        
        # Test database loading
        load_to_database(sample_data)
        print("✅ Database loading test passed!")
        
        return True
        
    except Exception as e:
        print(f"❌ Loading test failed: {str(e)}")
        return False

def test_full_pipeline():
    """Test the complete ETL pipeline with real API data"""
    print("\n🚀 Testing complete ETL pipeline with live API...")
    
    # Test extraction with real API
    raw_data = test_extract()
    if not raw_data:
        print("❌ Pipeline test failed at extraction stage")
        return False
    
    # Test transformation with real API data
    print("\n🔄 Testing transformation with real API data...")
    try:
        from scripts.transform_weather import transform_weather_data
        
        # Mock context for testing
        class MockTaskInstance:
            def xcom_push(self, key, value):
                print(f"XCom Push - {key}: {type(value).__name__}")
            
            def xcom_pull(self, key):
                return raw_data  # Use real API data
        
        transformed_data = transform_weather_data(raw_data, **{'task_instance': MockTaskInstance()})
        
        if transformed_data and 'temperature' in transformed_data and 'humidity' in transformed_data:
            print("✅ Real API transformation test passed!")
            print(f"   Real Temperature: {transformed_data['temperature']}°C")
            print(f"   Real Humidity: {transformed_data['humidity']}%")
            print(f"   Real City: {transformed_data['city']}")
        else:
            print("❌ Real API transformation test failed")
            return False
            
    except Exception as e:
        print(f"❌ Real API transformation test failed: {str(e)}")
        return False
    
    # Test loading with real transformed data
    print("\n💾 Testing loading with real transformed data...")
    try:
        from scripts.load_weather import load_to_csv, load_to_database
        
        # Test CSV loading
        load_to_csv(transformed_data)
        print("✅ Real data CSV loading test passed!")
        
        # Test database loading
        load_to_database(transformed_data)
        print("✅ Real data database loading test passed!")
        
    except Exception as e:
        print(f"❌ Real data loading test failed: {str(e)}")
        return False
    
    print("\n🎉 Complete ETL pipeline test with real API data passed!")
    return True

def test_config():
    """Test configuration loading"""
    print("⚙️ Testing configuration...")
    
    try:
        from config.config import WEATHERAPI_API_KEY, DEFAULT_CITY, DB_CONFIG
        
        print(f"   Default City: {DEFAULT_CITY}")
        print(f"   Database Type: {DB_CONFIG['type']}")
        
        if WEATHERAPI_API_KEY and WEATHERAPI_API_KEY != 'your_api_key_here':
            print("✅ API key configured!")
            print(f"   API Key: {WEATHERAPI_API_KEY[:10]}...{WEATHERAPI_API_KEY[-4:]}")  # Show partial key for verification
            return True
        else:
            print("⚠️  API key not configured - update .env file")
            return False
        
    except Exception as e:
        print(f"❌ Configuration test failed: {str(e)}")
        return False

def main():
    """Run all tests"""
    print("🧪 Weather ETL Pipeline Test Suite")
    print("=" * 50)
    
    # Test configuration first
    config_success = test_config()
    
    # Test individual components
    print("\n📋 Running component tests...")
    
    # Always try live API testing first if API key is available
    print("\n🔍 Checking API availability...")
    
    if config_success:
        print("🌐 API key found - running live API tests...")
        try:
            # Try full pipeline with real API
            pipeline_success = test_full_pipeline()
            if pipeline_success:
                print("\n🎉 All tests completed successfully with live API data!")
            else:
                print("\n⚠️  Some live API tests failed, running offline tests...")
                test_transform()
                test_load()
        except Exception as e:
            print(f"\n❌ Live API test failed: {str(e)}")
            print("   Running offline tests with mock data...")
            test_transform()
            test_load()
    else:
        print("⚠️  No valid API key found - running offline tests only...")
        test_transform()
        test_load()
    
    print("\n✅ Test suite completed!")
    print("\n💡 Tips:")
    print("   - API key is configured and should work for live testing")
    print("   - Check data/weather_data.csv for output")
    print("   - Check weather_data.db for database records")
    print("   - Run 'python scripts/extract_weather.py' to test API directly")

if __name__ == "__main__":
    main()
