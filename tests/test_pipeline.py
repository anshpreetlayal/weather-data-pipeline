# tests/test_pipeline.py
"""
Unit tests for the weather data pipeline.
Tests each component: ingestion, transformation, and storage.
"""

import unittest
import sys
import os
from datetime import datetime

# Add parent directory to path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.ingestion import fetch_weather_data
from src.transformation import transform_weather_data, validate_and_clean
from src.config import OPENWEATHER_API_KEY

class TestIngestion(unittest.TestCase):
    """
    Tests for the data ingestion module.
    """
    
    def test_fetch_weather_data_success(self):
        """Test that we can successfully fetch weather data."""
        result = fetch_weather_data('Toronto')
        
        # Check that we got a response
        self.assertIsNotNone(result, "API should return data")
        
        # Check that essential fields exist
        self.assertIn('name', result, "Response should contain city name")
        self.assertIn('main', result, "Response should contain main weather data")
        self.assertIn('weather', result, "Response should contain weather description")
    
    def test_fetch_weather_data_invalid_city(self):
        """Test behavior with invalid city name."""
        result = fetch_weather_data('InvalidCityName12345XYZ')
        
        # Should return None for invalid city
        self.assertIsNone(result, "Invalid city should return None")
    
    def test_api_key_exists(self):
        """Test that API key is configured."""
        self.assertIsNotNone(OPENWEATHER_API_KEY, "API key must be configured")
        self.assertTrue(len(OPENWEATHER_API_KEY) > 0, "API key must not be empty")

class TestTransformation(unittest.TestCase):
    """
    Tests for the data transformation module.
    """
    
    def setUp(self):
        """Set up sample data for testing."""
        self.sample_data = {
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
    
    def test_transform_weather_data(self):
        """Test that transformation produces correct output."""
        df = transform_weather_data(self.sample_data)
        
        # Check that DataFrame is created
        self.assertIsNotNone(df, "Transformation should return DataFrame")
        self.assertEqual(len(df), 1, "Should have exactly 1 row")
        
        # Check that essential columns exist
        self.assertIn('city', df.columns, "Should have city column")
        self.assertIn('temperature', df.columns, "Should have temperature column")
        self.assertIn('humidity', df.columns, "Should have humidity column")
        
        # Check that values are correct
        self.assertEqual(df.iloc[0]['city'], 'Toronto', "City should be Toronto")
        self.assertEqual(df.iloc[0]['temperature'], 7.2, "Temperature should match")
        self.assertEqual(df.iloc[0]['humidity'], 75, "Humidity should match")
    
    def test_validation_temperature_range(self):
        """Test that temperature validation works."""
        import pandas as pd
        
        # Create test DataFrame with extreme temperature
        test_df = pd.DataFrame([{
            'timestamp': datetime.now(),
            'city': 'Test',
            'temperature': 100.0,  # Unrealistic temperature
            'humidity': 50,
            'pressure': 1013,
            'wind_speed': 5.0,
            'is_valid': True
        }])
        
        # Validate
        validated_df = validate_and_clean(test_df)
        
        # Should mark as invalid
        self.assertFalse(validated_df.iloc[0]['is_valid'], 
                        "Extreme temperature should be marked invalid")
    
    def test_validation_humidity_range(self):
        """Test that humidity validation works."""
        import pandas as pd
        
        # Create test DataFrame with invalid humidity
        test_df = pd.DataFrame([{
            'timestamp': datetime.now(),
            'city': 'Test',
            'temperature': 20.0,
            'humidity': 150,  # Invalid (> 100%)
            'pressure': 1013,
            'wind_speed': 5.0,
            'is_valid': True
        }])
        
        # Validate
        validated_df = validate_and_clean(test_df)
        
        # Humidity should be capped at 100
        self.assertLessEqual(validated_df.iloc[0]['humidity'], 100,
                            "Humidity should be capped at 100%")

class TestStorage(unittest.TestCase):
    """
    Tests for the data storage module.
    Note: These tests require a running PostgreSQL database.
    """
    
    def test_database_connection(self):
        """Test that we can connect to the database."""
        from src.storage import WeatherDatabase
        
        db = WeatherDatabase()
        result = db.connect()
        
        if result:
            db.disconnect()
            self.assertTrue(result, "Should connect to database")
        else:
            self.skipTest("Database not available for testing")

class TestEndToEndPipeline(unittest.TestCase):
    """
    End-to-end integration tests.
    """
    
    def test_full_pipeline(self):
        """Test the complete ETL pipeline."""
        # 1. EXTRACT
        raw_data = fetch_weather_data('Toronto')
        self.assertIsNotNone(raw_data, "Should fetch data")
        
        # 2. TRANSFORM
        df = transform_weather_data(raw_data)
        self.assertIsNotNone(df, "Should transform data")
        self.assertEqual(len(df), 1, "Should have 1 row")
        
        # 3. LOAD (skip if database not available)
        try:
            from src.storage import WeatherDatabase
            db = WeatherDatabase()
            if db.connect():
                rows = db.insert_weather_data(df)
                db.disconnect()
                self.assertGreater(rows, 0, "Should insert at least 1 row")
        except:
            self.skipTest("Database not available for integration test")

def run_tests():
    """
    Run all tests and display results.
    """
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestIngestion))
    suite.addTests(loader.loadTestsFromTestCase(TestTransformation))
    suite.addTests(loader.loadTestsFromTestCase(TestStorage))
    suite.addTests(loader.loadTestsFromTestCase(TestEndToEndPipeline))
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)