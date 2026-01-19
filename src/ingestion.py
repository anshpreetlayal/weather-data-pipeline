# src/ingestion.py
"""
Data ingestion module for fetching weather data from OpenWeatherMap API.
This is the 'Extract' part of the ETL pipeline.
"""

import requests
import json
from datetime import datetime
from config import (
    OPENWEATHER_API_KEY, 
    OPENWEATHER_BASE_URL, 
    CITIES_TO_TRACK
)

def fetch_weather_data(city):
    """
    Fetches current weather data for a specific city.
    
    Parameters:
        city (str): Name of the city (e.g., "Toronto", "New York")
    
    Returns:
        dict: Weather data as a dictionary, or None if request failed
    """
    params = {
        'q': city,
        'appid': OPENWEATHER_API_KEY,
        'units': 'metric'  # Celsius, m/s for wind
    }
    
    try:
        response = requests.get(OPENWEATHER_BASE_URL, params=params, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Error fetching data for {city}: Status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Exception while fetching data for {city}: {str(e)}")
        return None

def fetch_all_cities():
    """
    Fetches weather data for all configured cities.
    
    Returns:
        list: List of dictionaries containing weather data for each city
    """
    results = []
    
    print(f"📡 Fetching weather data for {len(CITIES_TO_TRACK)} cities...")
    
    for city in CITIES_TO_TRACK:
        print(f"  🔍 Fetching {city}...", end=" ")
        data = fetch_weather_data(city)
        
        if data:
            results.append(data)
            temp = data['main']['temp']
            weather = data['weather'][0']['description']
            print(f"✅ {temp}°C, {weather}")
        else:
            print("❌ Failed")
    
    print(f"\n✅ Successfully fetched data for {len(results)}/{len(CITIES_TO_TRACK)} cities")
    return results

def save_to_json(data, filename=None):
    """
    Saves weather data to a JSON file.
    Useful for testing before setting up database.
    
    Parameters:
        data: Weather data (dict or list of dicts)
        filename: Output filename (default: uses timestamp)
    """
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"weather_data_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"💾 Data saved to {filename}")

# Test the ingestion module
if __name__ == "__main__":
    print("\n🚀 Weather Data Ingestion Test")
    print("=" * 60)
    
    # Fetch data for all configured cities
    weather_data = fetch_all_cities()
    
    # Save to JSON file
    if weather_data:
        save_to_json(weather_data)
        print("\n✅ Ingestion test completed successfully!")
    else:
        print("\n❌ Ingestion test failed!")