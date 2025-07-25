CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    city VARCHAR(100) NOT NULL,
    temperature FLOAT NOT NULL,
    humidity INTEGER NOT NULL,
    wind_speed FLOAT NOT NULL,
    weather_description VARCHAR(200),
    pressure FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_weather_city ON weather_data(city);
