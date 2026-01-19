-- sql/schema.sql
-- Database schema for weather data pipeline
-- This creates the table structure for storing weather data

-- Drop table if it exists
DROP TABLE IF EXISTS weather_data CASCADE;

-- Create the main weather data table
CREATE TABLE weather_data (
    -- Primary key: auto-incrementing ID
    id SERIAL PRIMARY KEY,
    
    -- Timestamp when data was collected
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Location information
    city VARCHAR(100) NOT NULL,
    country_code CHAR(2),  -- e.g., 'CA' for Canada
    latitude DECIMAL(10, 7),  -- e.g., 43.7001
    longitude DECIMAL(10, 7),  -- e.g., -79.4163
    
    -- Temperature data (in Celsius)
    temperature DECIMAL(5, 2),  -- Current temperature
    feels_like DECIMAL(5, 2),   -- Feels like temperature
    temp_min DECIMAL(5, 2),     -- Minimum temperature
    temp_max DECIMAL(5, 2),     -- Maximum temperature
    
    -- Atmospheric data
    pressure INTEGER,           -- Atmospheric pressure (hPa)
    humidity INTEGER,           -- Humidity percentage (0-100)
    
    -- Weather condition
    weather_main VARCHAR(50),        -- Main weather category (e.g., 'Rain', 'Clouds')
    weather_description VARCHAR(200), -- Detailed description (e.g., 'light rain')
    weather_icon VARCHAR(10),        -- Icon code from API
    
    -- Wind data
    wind_speed DECIMAL(5, 2),   -- Wind speed (m/s)
    wind_direction INTEGER,      -- Wind direction (degrees)
    
    -- Cloud coverage
    cloudiness INTEGER,          -- Cloud coverage percentage (0-100)
    
    -- Visibility
    visibility INTEGER,          -- Visibility in meters
    
    -- API response metadata
    api_timestamp INTEGER,       -- Unix timestamp from API
    timezone_offset INTEGER,     -- Timezone offset in seconds
    
    -- Data quality
    data_source VARCHAR(50) DEFAULT 'OpenWeatherMap',
    is_valid BOOLEAN DEFAULT TRUE
);

-- Create indexes for faster queries
-- Index on city and timestamp (most common query pattern)
CREATE INDEX idx_city_timestamp ON weather_data(city, timestamp DESC);

-- Index on timestamp alone (for time-based queries)
CREATE INDEX idx_timestamp ON weather_data(timestamp DESC);

-- Index on city alone (for city-based queries)
CREATE INDEX idx_city ON weather_data(city);

-- Create a view for the latest weather data per city
CREATE OR REPLACE VIEW latest_weather AS
SELECT DISTINCT ON (city)
    *
FROM weather_data
ORDER BY city, timestamp DESC;

-- Comments on table and columns (PostgreSQL documentation)
COMMENT ON TABLE weather_data IS 'Stores historical weather data from OpenWeatherMap API';
COMMENT ON COLUMN weather_data.timestamp IS 'When this record was inserted into the database';
COMMENT ON COLUMN weather_data.api_timestamp IS 'Unix timestamp from the API response';
COMMENT ON COLUMN weather_data.is_valid IS 'Flag for data quality checks';

-- Create a function to clean old data (optional)
CREATE OR REPLACE FUNCTION cleanup_old_weather_data(days_to_keep INTEGER)
RETURNS INTEGER AS $$
DECLARE
    rows_deleted INTEGER;
BEGIN
    DELETE FROM weather_data 
    WHERE timestamp < (CURRENT_TIMESTAMP - (days_to_keep || ' days')::INTERVAL);
    
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;
    RETURN rows_deleted;
END;
$$ LANGUAGE plpgsql;

-- Example usage of cleanup function (commented out):
-- SELECT cleanup_old_weather_data(90);  -- Deletes data older than 90 days