"""
Test script to verify that the OpenWeatherMap API key works correctly.
"""

import requests
import json
from datetime import datetime
from config import OPENWEATHER_API_KEY, OPENWEATHER_BASE_URL, DEFAULT_CITY

def test_api_connection():
    """
    tests the API connection by fetching weather data for the default city
    Returns:
    bool: true if successful, false otherwise
    """

    print("=" * 60)
    print(" testing OpenWeatherMap API connection")
    print("=" * 60)

    # build the request URL with parameters
    params = {
        'q': DEFAULT_CITY, # city name
        'appid': OPENWEATHER_API_KEY, # api key
        'units': 'metric'  #gets temp in celsius
    }

    print(f"\n Sending requst to OpenWeatherMap API...")
    print(f" city: {DEFAULT_CITY}")
    print(f" API Key: {OPENWEATHER_API_KEY[:10]}...(hidden for security)")
     

    try:
        # make the http get request
        response = requests.get(OPENWEATHER_BASE_URL, params_params, timeout=10)

        #check the http status code
        # 200 = success, 401= invalid API key, 404 = city not found
        print(f"\n📊 Response Status Code: {response.status_code}")
        
        if response.status_code == 200:
            # Success! Parse the JSON response
            data = response.json()
            
            print("\n✅ API CONNECTION SUCCESSFUL!")
            print("\n" + "=" * 60)
            print("📋 WEATHER DATA RECEIVED:")
            print("=" * 60)
            
            # Extract and display key information
            print(f"\n🌍 Location: {data['name']}, {data['sys']['country']}")
            print(f"🌡️  Temperature: {data['main']['temp']}°C")
            print(f"🌡️  Feels Like: {data['main']['feels_like']}°C")
            print(f"💧 Humidity: {data['main']['humidity']}%")
            print(f"🌬️  Pressure: {data['main']['pressure']} hPa")
            print(f"☁️  Weather: {data['weather'][0]['main']} - {data['weather'][0]['description']}")
            print(f"💨 Wind Speed: {data['wind']['speed']} m/s")
            
            # Display timestamp
            timestamp = datetime.fromtimestamp(data['dt'])
            print(f"🕐 Data Timestamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            
            print("\n" + "=" * 60)
            print("📄 RAW JSON RESPONSE:")
            print("=" * 60)
            # Pretty print the full JSON response
            print(json.dumps(data, indent=2))
            
            return True
            
        elif response.status_code == 401:
            print("\n❌ ERROR: Invalid API Key!")
            print("Please check that your API key in .env is correct.")
            print(f"Current key: {OPENWEATHER_API_KEY[:10]}...")
            return False
            
        elif response.status_code == 404:
            print(f"\n❌ ERROR: City '{DEFAULT_CITY}' not found!")
            print("Please check the city name in your .env file.")
            return False
            
        elif response.status_code == 429:
            print("\n❌ ERROR: Too many requests!")
            print("You've exceeded the API rate limit. Please wait and try again.")
            return False
            
        else:
            print(f"\n❌ ERROR: Unexpected status code {response.status_code}")
            print("Response:", response.text)
            return False
            
    except requests.exceptions.Timeout:
        print("\n❌ ERROR: Request timed out!")
        print("The API took too long to respond. Check your internet connection.")
        return False
        
    except requests.exceptions.ConnectionError:
        print("\n❌ ERROR: Connection failed!")
        print("Could not connect to the API. Check your internet connection.")
        return False
        
    except Exception as e:
        print(f"\n❌ ERROR: An unexpected error occurred!")
        print(f"Error details: {str(e)}")
        return False

def test_multiple_cities():
    """
    Tests the API with multiple cities to ensure it works consistently.
    """
    cities = ['Toronto', 'London', 'Tokyo', 'New York']
    
    print("\n" + "=" * 60)
    print("🌍 Testing Multiple Cities")
    print("=" * 60)
    
    results = {}
    
    for city in cities:
        print(f"\n🔍 Testing {city}...", end=" ")
        
        params = {
            'q': city,
            'appid': OPENWEATHER_API_KEY,
            'units': 'metric'
        }
        
        try:
            response = requests.get(OPENWEATHER_BASE_URL, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                temp = data['main']['temp']
                weather = data['weather'][0]['description']
                results[city] = {'temp': temp, 'weather': weather, 'success': True}
                print(f"✅ {temp}°C, {weather}")
            else:
                results[city] = {'success': False, 'error': response.status_code}
                print(f"❌ Error {response.status_code}")
                
        except Exception as e:
            results[city] = {'success': False, 'error': str(e)}
            print(f"❌ {str(e)}")
    
    # Summary
    successful = sum(1 for r in results.values() if r.get('success', False))
    print(f"\n📊 Summary: {successful}/{len(cities)} cities tested successfully")
    
    return results

def main():
    """
    Main function that runs all tests.
    """
    print("\n")
    print("🚀 Weather Data Pipeline - API Test Suite")
    print("=" * 60)
    
    # Test 1: Basic API connection
    success = test_api_connection()
    
    if success:
        print("\n" + "=" * 60)
        input("Press Enter to test multiple cities... ")
        
        # Test 2: Multiple cities
        test_multiple_cities()
        
        print("\n" + "=" * 60)
        print("✅ All tests completed!")
        print("=" * 60)
        print("\n💡 Next steps:")
        print("1. Your API key is working correctly")
        print("2. You can now proceed to build the data pipeline")
        print("3. Run 'python src/ingestion.py' to start fetching data")
    else:
        print("\n" + "=" * 60)
        print("❌ API test failed!")
        print("=" * 60)
        print("\n🔧 Troubleshooting steps:")
        print("1. Check that your .env file exists")
        print("2. Verify your API key is correct")
        print("3. Make sure you have internet connection")
        print("4. Wait a few minutes if you hit rate limits")

if __name__ == "__main__":
    # This runs when you execute: python src/test_api.py
    main()