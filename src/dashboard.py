# src/storage.py
"""
Data storage module.
This is the 'Load' part of the ETL pipeline.
Handles all database operations for storing weather data.
"""

import psycopg2
from psycopg2 import sql, extras
import pandas as pd
import logging
from config import DB_CONFIG

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherDatabase:
    """
    Class to handle all database operations for weather data.
    """
    
    def __init__(self, config=None):
        """
        Initialize database connection.
        
        Parameters:
            config (dict): Database configuration dictionary
                         If None, uses DB_CONFIG from config.py
        """
        self.config = config or DB_CONFIG
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish connection to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            self.cursor = self.connection.cursor()
            logger.info("✅ Connected to database")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {str(e)}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("🔌 Disconnected from database")
    
    def insert_weather_data(self, df):
        """
        Insert weather data from DataFrame into database.
        
        Parameters:
            df (pandas.DataFrame): Weather data to insert
        
        Returns:
            int: Number of rows inserted
        """
        
        if df is None or df.empty:
            logger.warning("⚠️ No data to insert")
            return 0
        
        try:
            # Prepare INSERT query
            insert_query = """
            INSERT INTO weather_data (
                timestamp, city, country_code, latitude, longitude,
                temperature, feels_like, temp_min, temp_max,
                pressure, humidity,
                weather_main, weather_description, weather_icon,
                wind_speed, wind_direction,
                cloudiness, visibility,
                api_timestamp, timezone_offset,
                data_source, is_valid
            ) VALUES (
                %(timestamp)s, %(city)s, %(country_code)s, %(latitude)s, %(longitude)s,
                %(temperature)s, %(feels_like)s, %(temp_min)s, %(temp_max)s,
                %(pressure)s, %(humidity)s,
                %(weather_main)s, %(weather_description)s, %(weather_icon)s,
                %(wind_speed)s, %(wind_direction)s,
                %(cloudiness)s, %(visibility)s,
                %(api_timestamp)s, %(timezone_offset)s,
                %(data_source)s, %(is_valid)s
            )
            """
            
            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')
            
            # Insert each record
            rows_inserted = 0
            for record in records:
                self.cursor.execute(insert_query, record)
                rows_inserted += 1
            
            # Commit the transaction
            self.connection.commit()
            
            logger.info(f"✅ Inserted {rows_inserted} rows into database")
            return rows_inserted
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"❌ Error inserting data: {str(e)}")
            return 0
    
    def get_latest_weather(self, city=None):
        """
        Get the most recent weather data.
        
        Parameters:
            city (str): Optional city name to filter by
        
        Returns:
            pandas.DataFrame: Latest weather data
        """
        
        try:
            if city:
                query = """
                SELECT * FROM weather_data 
                WHERE city = %s 
                ORDER BY timestamp DESC 
                LIMIT 1
                """
                df = pd.read_sql(query, self.connection, params=(city,))
            else:
                query = "SELECT * FROM latest_weather"
                df = pd.read_sql(query, self.connection)
            
            logger.info(f"✅ Retrieved {len(df)} latest weather records")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error retrieving data: {str(e)}")
            return None
    
    def get_historical_data(self, city, days=7):
        """
        Get historical weather data for a city.
        
        Parameters:
            city (str): City name
            days (int): Number of days of history to retrieve
        
        Returns:
            pandas.DataFrame: Historical weather data
        """
        
        try:
            query = """
            SELECT * FROM weather_data 
            WHERE city = %s 
            AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '%s days'
            ORDER BY timestamp DESC
            """
            
            df = pd.read_sql(query, self.connection, params=(city, days))
            logger.info(f"✅ Retrieved {len(df)} historical records for {city}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error retrieving historical data: {str(e)}")
            return None
    
    def get_statistics(self, city, days=30):
        """
        Get weather statistics for a city.
        
        Parameters:
            city (str): City name
            days (int): Number of days to calculate statistics for
        
        Returns:
            dict: Dictionary of statistics
        """
        
        try:
            query = """
            SELECT 
                COUNT(*) as record_count,
                AVG(temperature) as avg_temp,
                MIN(temperature) as min_temp,
                MAX(temperature) as max_temp,
                AVG(humidity) as avg_humidity,
                AVG(pressure) as avg_pressure,
                AVG(wind_speed) as avg_wind_speed
            FROM weather_data
            WHERE city = %s
            AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '%s days'
            """
            
            self.cursor.execute(query, (city, days))
            result = self.cursor.fetchone()
            
            if result:
                stats = {
                    'record_count': result[0],
                    'avg_temp': round(result[1], 2) if result[1] else None,
                    'min_temp': round(result[2], 2) if result[2] else None,
                    'max_temp': round(result[3], 2) if result[3] else None,
                    'avg_humidity': round(result[4], 2) if result[4] else None,
                    'avg_pressure': round(result[5], 2) if result[5] else None,
                    'avg_wind_speed': round(result[6], 2) if result[6] else None
                }
                
                logger.info(f"✅ Calculated statistics for {city}")
                return stats
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error calculating statistics: {str(e)}")
            return None

def save_weather_data(df, db_config=None):
    """
    Convenience function to save weather data to database.
    
    Parameters:
        df (pandas.DataFrame): Weather data to save
        db_config (dict): Optional database configuration
    
    Returns:
        bool: True if successful, False otherwise
    """
    
    db = WeatherDatabase(db_config)
    
    try:
        if db.connect():
            rows = db.insert_weather_data(df)
            db.disconnect()
            return rows > 0
        return False
    except Exception as e:
        logger.error(f"❌ Error saving data: {str(e)}")
        db.disconnect()
        return False

# Test the storage module
if __name__ == "__main__":
    print("\n🧪 Testing Storage Module")
    print("=" * 60)
    
    # Create test database connection
    db = WeatherDatabase()
    
    if db.connect():
        print("✅ Database connection successful!")
        
        # Test getting latest weather
        print("\n📊 Testing get_latest_weather()...")
        latest = db.get_latest_weather()
        if latest is not None:
            print(f"Found {len(latest)} latest records")
            print(latest[['city', 'temperature', 'humidity']].head())
        
        # Test statistics
        print("\n📊 Testing get_statistics()...")
        stats = db.get_statistics('Toronto', days=7)
        if stats:
            print("Statistics for Toronto (last 7 days):")
            for key, value in stats.items():
                print(f"  {key}: {value}")
        
        db.disconnect()
        print("\n✅ Storage test completed!")
    else:
        print("❌ Could not connect to database")
        print("Make sure PostgreSQL is running and configured correctly")