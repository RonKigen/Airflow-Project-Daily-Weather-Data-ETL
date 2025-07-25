#!/usr/bin/env python3
"""
Simple API test script to verify WeatherAPI.com is working
"""

import sys
import os
import requests
from datetime import datetime

# Add project paths
sys.path.append(os.path.dirname(__file__))

def test_direct_api():
    """Test WeatherAPI.com directly"""
    from config.config import WEATHERAPI_API_KEY, WEATHERAPI_BASE_URL
    
    cities = ['London', 'New York', 'Tokyo', 'Sydney']
    
    print("🌤️  WeatherAPI.com Direct Test")
    print("=" * 40)
    
    for city in cities:
        try:
            params = {
                'key': WEATHERAPI_API_KEY,
                'q': city,
                'aqi': 'no'
            }
            
            response = requests.get(WEATHERAPI_BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            print(f"✅ {city}:")
            print(f"   Temperature: {data['current']['temp_c']}°C")
            print(f"   Feels like: {data['current']['feelslike_c']}°C")
            print(f"   Humidity: {data['current']['humidity']}%")
            print(f"   Weather: {data['current']['condition']['text']}")
            print(f"   Wind: {data['current']['wind_kph']} kph")
            print()
            
        except Exception as e:
            print(f"❌ {city}: {str(e)}")
    
    print("🎉 API test completed!")

if __name__ == "__main__":
    test_direct_api()
