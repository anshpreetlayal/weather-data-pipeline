# src/transformation.py
"""
Data transformation module.
This is the 'Transform' part of the ETL pipeline.
Cleans and structures raw API data into a format ready for database storage.
"""

import pandas as pd
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_weather_data(raw_data):
    """
    Transforms raw weather API response into a structured DataFrame.
    
    Parameters:
        raw_data (dict): Raw JSON response from OpenWeatherMap API
    
    Returns:
        pandas.DataFrame: Cleaned and structured weather data
    
    Example input (raw_data):
    {
        "name": "Toronto",
        "sys": {"country": "CA"},
        "coord": {"lat": 43.7, "lon": -79.42},
        "main": {"temp": 7.2, "feels_like": 4.3, ...},
        "weather": [{"main": "Clouds", "description": "overcast clouds"}],
        ...
    }
    """
    
    try:
        # Extract data with safe navigation (handle missing fields)
        transformed = {
            # Timestamp
            'timestamp': datetime.now(),
            
            # Location
            'city': raw_data.get('name', 'Unknown'),
            'country_code': raw_data.get('sys', {}).get('country', None),
            'latitude': raw_data.get('coord', {}).get('lat', None),
            'longitude': raw_data.get('coord', {}).get('lon', None),
            
            # Temperature (already in Celsius because we used units=metric)
            'temperature': raw_data.get('main', {}).get('temp', None),
            'feels_like': raw_data.get('main', {}).get('feels_like', None),
            'temp_min': raw_data.get('main', {}).get('temp_min', None),
            'temp_max': raw_data.get('main', {}).get('temp_max', None),
            
            # Atmospheric
            'pressure': raw_data.get('main', {}).get('pressure', None),
            'humidity': raw_data.get('main', {}).get('humidity', None),
            
            # Weather condition
            # weather is a list, we take the first element
            'weather_main': raw_data.get('weather', [{}])[0].get('main', None),
            'weather_description': raw_data.get('weather', [{}])[0].get('description', None),
            'weather_icon': raw_data.get('weather', [{}])[0].get('icon', None),
            
            # Wind
            'wind_speed': raw_data.get('wind', {}).get('speed', None),
            'wind_direction': raw_data.get('wind', {}).get('deg', None),
            
            # Clouds
            'cloudiness': raw_data.get('clouds', {}).get('all', None),
            
            # Visibility
            'visibility': raw_data.get('visibility', None),
            
            # API metadata
            'api_timestamp': raw_data.get('dt', None),
            'timezone_offset': raw_data.get('timezone', None),
            
            # Data quality
            'data_source': 'OpenWeatherMap',
            'is_valid': True  # We'll add validation logic later
        }
        
        # Create DataFrame from single record
        df = pd.DataFrame([transformed])
        
        # Data validation and cleaning
        df = validate_and_clean(df)
        
        logger.info(f"✅ Transformed data for {transformed['city']}")
        return df
        
    except Exception as e:
        logger.error(f"❌ Error transforming data: {str(e)}")
        return None

def validate_and_clean(df):
    """
    Validates and cleans the transformed data.
    
    Parameters:
        df (pandas.DataFrame): Transformed weather data
    
    Returns:
        pandas.DataFrame: Validated and cleaned data
    """
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # 1. Check for reasonable temperature ranges (-50°C to 60°C)
    if df['temperature'].notna().any():
        temp = df['temperature'].iloc[0]
        if temp < -50 or temp > 60:
            logger.warning(f"⚠️ Unusual temperature detected: {temp}°C")
            df['is_valid'] = False
    
    # 2. Humidity should be 0-100%
    if df['humidity'].notna().any():
        humidity = df['humidity'].iloc[0]
        if humidity < 0 or humidity > 100:
            logger.warning(f"⚠️ Invalid humidity: {humidity}%")
            df.loc[df['humidity'] < 0, 'humidity'] = 0
            df.loc[df['humidity'] > 100, 'humidity'] = 100
    
    # 3. Pressure should be reasonable (900-1100 hPa)
    if df['pressure'].notna().any():
        pressure = df['pressure'].iloc[0]
        if pressure < 900 or pressure > 1100:
            logger.warning(f"⚠️ Unusual pressure: {pressure} hPa")
    
    # 4. Wind speed shouldn't be negative
    if df['wind_speed'].notna().any():
        if df['wind_speed'].iloc[0] < 0:
            logger.warning(f"⚠️ Negative wind speed detected, setting to 0")
            df['wind_speed'] = 0
    
    # 5. Round decimal values to 2 places
    decimal_columns = ['temperature', 'feels_like', 'temp_min', 'temp_max', 
                      'wind_speed', 'latitude', 'longitude']
    for col in decimal_columns:
        if col in df.columns:
            df[col] = df[col].round(2)
    
    logger.info("✅ Data validation completed")
    return df

def transform_multiple_cities(raw_data_list):
    """
    Transforms weather data for multiple cities.
    
    Parameters:
        raw_data_list (list): List of raw API responses
    
    Returns:
        pandas.DataFrame: Combined DataFrame with all cities
    """
    
    all_dataframes = []
    
    for raw_data in raw_data_list:
        df = transform_weather_data(raw_data)
        if df is not None:
            all_dataframes.append(df)
    
    if all_dataframes:
        # Combine all DataFrames into one
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"✅ Transformed {len(combined_df)} records")
        return combined_df
    else:
        logger.error("❌ No data to transform")
        return None

def add_calculated_fields(df):
    """
    Adds calculated/derived fields to the DataFrame.
    
    Parameters:
        df (pandas.DataFrame): Weather data
    
    Returns:
        pandas.DataFrame: DataFrame with additional calculated fields
    """
    
    df = df.copy()
    
    # 1. Temperature difference (actual vs feels like)
    if 'temperature' in df.columns and 'feels_like' in df.columns:
        df['temp_feels_diff'] = (df['temperature'] - df['feels_like']).round(2)
    
    # 2. Temperature range
    if 'temp_max' in df.columns and 'temp_min' in df.columns:
        df['temp_range'] = (df['temp_max'] - df['temp_min']).round(2)
    
    # 3. Categorize temperature
    if 'temperature' in df.columns:
        df['temp_category'] = pd.cut(
            df['temperature'],
            bins=[-float('inf'), 0, 10, 20, 30, float('inf')],
            labels=['Freezing', 'Cold', 'Mild', 'Warm', 'Hot']
        )
    
    # 4. Categorize weather conditions
    if 'weather_main' in df.columns:
        weather_severity = {
            'Clear': 'Good',
            'Clouds': 'Fair',
            'Rain': 'Poor',
            'Drizzle': 'Fair',
            'Thunderstorm': 'Severe',
            'Snow': 'Poor',
            'Mist': 'Fair',
            'Fog': 'Poor'
        }
        df['weather_severity'] = df['weather_main'].map(weather_severity)
    
    # 5. Time of day (from timestamp)
    if 'timestamp' in df.columns:
        df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
        df['time_of_day'] = pd.cut(
            df['hour'],
            bins=[-1, 6, 12, 18, 24],
            labels=['Night', 'Morning', 'Afternoon', 'Evening']
        )
    
    logger.info("✅ Added calculated fields")
    return df

# Test the transformation module
if __name__ == "__main__":
    print("\n🧪 Testing Transformation Module")
    print("=" * 60)
    
    # Sample API response for testing
    sample_data = {
        "coord": {"lon": -79.42, "lat": 43.7},
        "weather": [
            {
                "id": 804,
                "main": "Clouds",
                "description": "overcast clouds",
                "icon": "04d"
            }
        ],
        "main": {
            "temp": 7.2,
            "feels_like": 4.3,
            "temp_min": 5.8,
            "temp_max": 8.9,
            "pressure": 1013,
            "humidity": 75
        },
        "wind": {"speed": 4.5, "deg": 250},
        "clouds": {"all": 90},
        "visibility": 10000,
        "dt": 1705776000,
        "sys": {"country": "CA"},
        "timezone": -18000,
        "name": "Toronto"
    }
    
    # Transform the data
    df = transform_weather_data(sample_data)
    
    if df is not None:
        print("\n📊 Transformed DataFrame:")
        print(df.T)  # Transpose to show as rows
        
        print("\n📊 With Calculated Fields:")
        df_enhanced = add_calculated_fields(df)
        print(df_enhanced[['city', 'temperature', 'temp_category', 
                          'weather_main', 'weather_severity']].T)
    
    print("\n✅ Transformation test completed!")