"""
Test cases for weather data loading
"""
import os
import tempfile
import sqlite3
from datetime import datetime
from scripts.load_weather import load_to_csv, load_to_database, create_database_engine


class TestWeatherLoading:
    """Test cases for weather data loading"""
    
    def test_load_to_csv(self, sample_transformed_data):
        """Test CSV file loading"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as temp_file:
            temp_path = temp_file.name
        
        try:
            # Temporarily patch the output path
            original_path = os.environ.get('DATA_OUTPUT_PATH')
            os.environ['DATA_OUTPUT_PATH'] = temp_path
            
            # Test loading
            load_to_csv(sample_transformed_data)
            
            # Verify file was created and contains data
            assert os.path.exists(temp_path)
            
            with open(temp_path, 'r') as f:
                content = f.read()
                assert 'London' in content
                assert '15.5' in content  # Temperature
                
        finally:
            # Cleanup
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            if original_path:
                os.environ['DATA_OUTPUT_PATH'] = original_path
            elif 'DATA_OUTPUT_PATH' in os.environ:
                del os.environ['DATA_OUTPUT_PATH']
    
    def test_load_to_database(self, sample_transformed_data):
        """Test database loading"""
        with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as temp_file:
            temp_db_path = temp_file.name
        
        try:
            # Create a temporary database
            conn = sqlite3.connect(temp_db_path)
            conn.execute('''
                CREATE TABLE weather_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME,
                    city TEXT,
                    country TEXT,
                    temperature REAL,
                    feels_like REAL,
                    humidity INTEGER,
                    pressure REAL,
                    wind_speed REAL,
                    wind_direction REAL,
                    weather_main TEXT,
                    weather_description TEXT,
                    visibility REAL,
                    sunrise DATETIME,
                    sunset DATETIME,
                    extraction_timestamp TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.close()
            
            # Temporarily patch the database configuration
            original_db_config = {}
            if hasattr(load_to_database, '__globals__'):
                from config import config
                original_db_config = config.DB_CONFIG.copy()
                config.DB_CONFIG['database'] = temp_db_path
            
            # Test loading
            load_to_database(sample_transformed_data)
            
            # Verify data was inserted
            conn = sqlite3.connect(temp_db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM weather_data WHERE city = 'London'")
            rows = cursor.fetchall()
            conn.close()
            
            assert len(rows) > 0
            assert 'London' in str(rows[0])
            
        finally:
            # Cleanup
            if os.path.exists(temp_db_path):
                os.unlink(temp_db_path)
            
            # Restore original config
            if original_db_config and hasattr(load_to_database, '__globals__'):
                from config import config
                config.DB_CONFIG.update(original_db_config)
    
    def test_create_database_engine(self):
        """Test database engine creation"""
        engine = create_database_engine()
        assert engine is not None
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            assert result.fetchone()[0] == 1
