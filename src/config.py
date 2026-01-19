""" configuration file for loadinf environment variables and settings.
central place for all the configuration in the project. """

import os
from dotenv import load_dontenv
from pathlib import Path

# Get the project root directory
# __file__ is the path to this config.py file
# .parent.parent goes up two levels: src/ -> weather-data-pipeline/
BASE_DIR = Path(__file__).resolve().parent.parent

# load environment variables from .env file
# This reads the .env file and makes variables available via os.gotenv()

load_dontenv(BASE_DIR / '.env')

# API configuration
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
OPENWEATHER_BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
DEFAULT_CITY = os.getenv('DEFAULT_CITY', 'Toronto')  # default city toronto

# Database configuration 
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'weather_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

# logging configuration
LOG_DIR = BASE_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True) # create logs directory if it doesnt exists
LOG_FILE = LOG_DIR / 'pipeline.log'

# data collection settings
FETCH_INTERVAL_MINUTES = 5   # how often to fetch the weather data
CITIES_TO_TRACK = ['Toronto', 'Montreal', 'Vancouver'] # cities to monitor

# Validate that required environment variables are set
def validate_config():
    """
    Check that all required config is present.
    Raises an error if something is missing
    """
    if not OPENWEATHER_API_KEY:
        raise ValueError(
            "❌ OPENWEATHER_API_KEY not found in .env file!\n"
            "Please create a .env file with your API key"
        )
    print("✅ configuration loaded successfully!")
    print(f"📍 Default city: {DEFAULT_CITY}")
    print(f"🔑 API Key: {OPENWEATHER_API_KEY[:10]}... (hidden)")


# Run validation when module is imported
if __name__ == "__main__":
    validate_config()

    