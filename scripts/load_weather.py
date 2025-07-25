import pandas as pd
import sqlite3
import psycopg2
import logging
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from typing import Dict, List, Any
from config.config import DB_CONFIG, DATA_OUTPUT_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_weather_data(**context):
    """
    Load transformed weather data to database and CSV file
    
    Args:
        **context: Airflow context
    """
    try:
        # Get transformed data from XCom
        transformed_data = context['task_instance'].xcom_pull(key='transformed_weather_data')
        
        if not transformed_data:
            raise ValueError("No transformed weather data found to load")
        
        logger.info("Starting weather data loading process")
        
        # Load to database
        load_to_database(transformed_data)
        
        # Load to CSV file
        load_to_csv(transformed_data)
        
        logger.info("Successfully completed weather data loading")
        
    except Exception as e:
        logger.error(f"Error during data loading: {str(e)}")
        raise

def load_to_database(data: Dict[str, Any]):
    """
    Load weather data to database (SQLite or PostgreSQL)
    
    Args:
        data (dict): Transformed weather data
    """
    try:
        engine = create_database_engine()
        
        # Create DataFrame
        df = pd.DataFrame([data])
        
        # Prepare data for database insertion
        df_db = prepare_data_for_db(df)
        
        # Load to database
        df_db.to_sql('weather_data', engine, if_exists='append', index=False)
        
        logger.info(f"Successfully loaded weather data to database for {data['city']}")
        
    except Exception as e:
        logger.error(f"Error loading data to database: {str(e)}")
        raise

def load_to_csv(data: Dict[str, Any]):
    """
    Load weather data to CSV file
    
    Args:
        data (dict): Transformed weather data
    """
    try:
        # Ensure data directory exists
        os.makedirs(os.path.dirname(DATA_OUTPUT_PATH), exist_ok=True)
        
        # Create DataFrame
        df = pd.DataFrame([data])
        
        # Check if file exists to determine if we need headers
        file_exists = os.path.exists(DATA_OUTPUT_PATH)
        
        # Append to CSV file
        df.to_csv(DATA_OUTPUT_PATH, mode='a', header=not file_exists, index=False)
        
        logger.info(f"Successfully appended weather data to CSV: {DATA_OUTPUT_PATH}")
        
    except Exception as e:
        logger.error(f"Error loading data to CSV: {str(e)}")
        raise

def load_multiple_weather_data(**context):
    """
    Load multiple weather data records
    
    Args:
        **context: Airflow context
    """
    try:
        all_transformed_data = context['task_instance'].xcom_pull(key='all_transformed_data')
        
        if not all_transformed_data:
            raise ValueError("No transformed weather data found to load")
        
        logger.info(f"Loading {len(all_transformed_data)} weather records")
        
        # Load to database
        load_multiple_to_database(all_transformed_data)
        
        # Load to CSV
        load_multiple_to_csv(all_transformed_data)
        
        logger.info("Successfully loaded all weather data")
        
    except Exception as e:
        logger.error(f"Error during multiple data loading: {str(e)}")
        raise

def load_multiple_to_database(data_list: List[Dict[str, Any]]):
    """
    Load multiple weather records to database
    
    Args:
        data_list (list): List of transformed weather data
    """
    try:
        engine = create_database_engine()
        
        # Create DataFrame
        df = pd.DataFrame(data_list)
        
        # Prepare data for database insertion
        df_db = prepare_data_for_db(df)
        
        # Load to database
        df_db.to_sql('weather_data', engine, if_exists='append', index=False)
        
        logger.info(f"Successfully loaded {len(data_list)} weather records to database")
        
    except Exception as e:
        logger.error(f"Error loading multiple records to database: {str(e)}")
        raise

def load_multiple_to_csv(data_list: List[Dict[str, Any]]):
    """
    Load multiple weather records to CSV file
    
    Args:
        data_list (list): List of transformed weather data
    """
    try:
        # Ensure data directory exists
        os.makedirs(os.path.dirname(DATA_OUTPUT_PATH), exist_ok=True)
        
        # Create DataFrame
        df = pd.DataFrame(data_list)
        
        # Check if file exists to determine if we need headers
        file_exists = os.path.exists(DATA_OUTPUT_PATH)
        
        # Append to CSV file
        df.to_csv(DATA_OUTPUT_PATH, mode='a', header=not file_exists, index=False)
        
        logger.info(f"Successfully appended {len(data_list)} weather records to CSV")
        
    except Exception as e:
        logger.error(f"Error loading multiple records to CSV: {str(e)}")
        raise

def create_database_engine():
    """
    Create database engine based on configuration
    
    Returns:
        sqlalchemy.Engine: Database engine
    """
    try:
        if DB_CONFIG['type'] == 'sqlite':
            # Create SQLite database
            db_path = DB_CONFIG.get('database', 'weather_data.db')
            engine = create_engine(f'sqlite:///{db_path}')
            
            # Create tables if they don't exist
            create_sqlite_tables(engine)
            
        elif DB_CONFIG['type'] == 'postgresql':
            # Create PostgreSQL connection string
            connection_string = (
                f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}"
                f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            engine = create_engine(connection_string)
            
            # Create tables if they don't exist
            create_postgres_tables(engine)
        
        else:
            raise ValueError(f"Unsupported database type: {DB_CONFIG['type']}")
        
        return engine
        
    except Exception as e:
        logger.error(f"Error creating database engine: {str(e)}")
        raise

def create_sqlite_tables(engine):
    """
    Create SQLite tables if they don't exist
    
    Args:
        engine: SQLite engine
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME NOT NULL,
        city TEXT NOT NULL,
        country TEXT,
        temperature REAL NOT NULL,
        feels_like REAL,
        humidity INTEGER NOT NULL,
        pressure REAL,
        wind_speed REAL NOT NULL,
        wind_direction REAL,
        weather_main TEXT,
        weather_description TEXT,
        visibility REAL,
        sunrise DATETIME,
        sunset DATETIME,
        extraction_timestamp TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data(timestamp);
    CREATE INDEX IF NOT EXISTS idx_weather_city ON weather_data(city);
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()

def create_postgres_tables(engine):
    """
    Create PostgreSQL tables if they don't exist
    
    Args:
        engine: PostgreSQL engine
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        city VARCHAR(100) NOT NULL,
        country VARCHAR(10),
        temperature FLOAT NOT NULL,
        feels_like FLOAT,
        humidity INTEGER NOT NULL,
        pressure FLOAT,
        wind_speed FLOAT NOT NULL,
        wind_direction FLOAT,
        weather_main VARCHAR(50),
        weather_description VARCHAR(200),
        visibility FLOAT,
        sunrise TIMESTAMP,
        sunset TIMESTAMP,
        extraction_timestamp TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_data(timestamp);
    CREATE INDEX IF NOT EXISTS idx_weather_city ON weather_data(city);
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()

def prepare_data_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame for database insertion
    
    Args:
        df (pd.DataFrame): Weather data DataFrame
        
    Returns:
        pd.DataFrame: Prepared DataFrame
    """
    df_db = df.copy()
    
    # Handle datetime columns
    datetime_columns = ['timestamp', 'sunrise', 'sunset']
    for col in datetime_columns:
        if col in df_db.columns:
            df_db[col] = pd.to_datetime(df_db[col], errors='coerce')
    
    # Handle None values in sunrise/sunset
    df_db['sunrise'] = df_db['sunrise'].where(pd.notnull(df_db['sunrise']), None)
    df_db['sunset'] = df_db['sunset'].where(pd.notnull(df_db['sunset']), None)
    
    return df_db

def get_weather_history(city: str = None, days: int = 7) -> pd.DataFrame:
    """
    Retrieve weather history from database
    
    Args:
        city (str): City name to filter by
        days (int): Number of days to look back
        
    Returns:
        pd.DataFrame: Historical weather data
    """
    try:
        engine = create_database_engine()
        
        query = """
        SELECT * FROM weather_data 
        WHERE timestamp >= datetime('now', '-{} days')
        """.format(days)
        
        if city:
            query += f" AND city = '{city}'"
        
        query += " ORDER BY timestamp DESC"
        
        df = pd.read_sql(query, engine)
        return df
        
    except Exception as e:
        logger.error(f"Error retrieving weather history: {str(e)}")
        raise

if __name__ == "__main__":
    # Test loading with sample data
    sample_data = {
        'timestamp': datetime.utcnow(),
        'city': 'London',
        'country': 'GB',
        'temperature': 15.5,
        'feels_like': 14.2,
        'humidity': 78,
        'pressure': 1013.2,
        'wind_speed': 3.2,
        'wind_direction': 250,
        'weather_main': 'Clouds',
        'weather_description': 'Scattered Clouds',
        'visibility': 10000,
        'sunrise': datetime.utcnow(),
        'sunset': datetime.utcnow(),
        'extraction_timestamp': datetime.utcnow().isoformat()
    }
    
    load_to_database(sample_data)
    load_to_csv(sample_data)
    print("Test loading completed successfully")
