from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
import os

# Add scripts directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))

from scripts.extract_weather import extract_weather_data, extract_multiple_cities
from scripts.transform_weather import transform_weather_data, transform_multiple_weather_data
from scripts.load_weather import load_weather_data, load_multiple_weather_data
from config.config import AIRFLOW_EMAIL_ON_FAILURE, AIRFLOW_EMAIL_ON_RETRY, AIRFLOW_RETRIES, AIRFLOW_RETRY_DELAY_MINUTES

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': AIRFLOW_EMAIL_ON_FAILURE,
    'email_on_retry': AIRFLOW_EMAIL_ON_RETRY,
    'retries': AIRFLOW_RETRIES,
    'retry_delay': timedelta(minutes=AIRFLOW_RETRY_DELAY_MINUTES),
    'catchup': False  # Don't run historical DAGs
}

# Create the DAG
dag = DAG(
    'daily_weather_etl',
    default_args=default_args,
    description='Daily Weather Data ETL Pipeline',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM UTC
    max_active_runs=1,
    catchup=False,
    tags=['weather', 'etl', 'daily', 'api']
)

# Task 1: Extract weather data from OpenWeather API
extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    op_kwargs={'city': 'London'},  # Default city, can be parameterized
    dag=dag,
    doc_md="""
    ## Extract Weather Data
    
    This task extracts current weather data from the OpenWeather API for a specified city.
    
    **Parameters:**
    - city: Name of the city to get weather data for (default: London)
    
    **Returns:**
    - Raw weather data in JSON format
    - Stores data in XCom for downstream tasks
    """
)

# Task 2: Transform the extracted data
transform_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_weather_data,
    dag=dag,
    doc_md="""
    ## Transform Weather Data
    
    This task transforms the raw weather data into a clean, structured format.
    
    **Operations:**
    - Extract relevant fields (temperature, humidity, wind speed, etc.)
    - Data quality checks and validation
    - Convert units and format data types
    - Clean text fields
    
    **Returns:**
    - Cleaned and structured weather data dictionary
    """
)

# Task 3: Load the transformed data to database and CSV
load_task = PythonOperator(
    task_id='load_weather_data',
    python_callable=load_weather_data,
    dag=dag,
    doc_md="""
    ## Load Weather Data
    
    This task loads the transformed weather data to both database and CSV file.
    
    **Operations:**
    - Insert data into SQLite/PostgreSQL database
    - Append data to CSV file for backup/analysis
    - Create tables if they don't exist
    
    **Outputs:**
    - Database record in weather_data table
    - CSV file entry in data/weather_data.csv
    """
)

# Task 4: Data quality check (optional)
data_quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command="""
    echo "Running data quality checks..."
    echo "Checking if data was loaded successfully..."
    python -c "
from scripts.load_weather import get_weather_history
import sys
try:
    df = get_weather_history(days=1)
    if len(df) > 0:
        print(f'✓ Data quality check passed. Found {len(df)} records from last 24 hours.')
        sys.exit(0)
    else:
        print('✗ Data quality check failed. No recent data found.')
        sys.exit(1)
except Exception as e:
    print(f'✗ Data quality check failed with error: {e}')
    sys.exit(1)
"
    """,
    dag=dag,
    doc_md="""
    ## Data Quality Check
    
    This task performs basic data quality checks on the loaded data.
    
    **Checks:**
    - Verify data was successfully loaded to database
    - Check for recent records in the last 24 hours
    - Validate data completeness
    """
)

# Task 5: Generate daily summary (optional)
generate_summary = PythonOperator(
    task_id='generate_daily_summary',
    python_callable=lambda **context: print(f"""
    📊 Daily Weather ETL Summary - {datetime.now().strftime('%Y-%m-%d')}
    
    ✓ Weather data successfully extracted from OpenWeather API
    ✓ Data transformed and cleaned
    ✓ Data loaded to database and CSV file
    ✓ Data quality checks passed
    
    Next run scheduled: {context['next_ds']}
    """),
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    doc_md="""
    ## Generate Daily Summary
    
    This task generates a summary of the daily ETL pipeline execution.
    
    **Outputs:**
    - Pipeline execution summary
    - Success/failure status
    - Next scheduled run information
    """
)

# Task 6: Send notification email on failure (optional)
failure_notification = EmailOperator(
    task_id='send_failure_notification',
    to=['admin@example.com'],  # Update with actual email
    subject='Weather ETL Pipeline Failed - {{ ds }}',
    html_content="""
    <h3>Weather ETL Pipeline Failure</h3>
    <p>The daily weather ETL pipeline failed on {{ ds }}.</p>
    <p><strong>Failed Task:</strong> {{ task_instance.task_id }}</p>
    <p><strong>Error:</strong> {{ task_instance.log_url }}</p>
    <p>Please check the Airflow logs for more details.</p>
    """,
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED
)

# Alternative DAG for multiple cities (commented out by default)
"""
# Extract weather data for multiple cities
extract_multiple_cities_task = PythonOperator(
    task_id='extract_multiple_cities',
    python_callable=extract_multiple_cities,
    op_kwargs={'cities': ['London', 'New York', 'Tokyo', 'Sydney', 'Mumbai']},
    dag=dag
)

# Transform multiple cities data
transform_multiple_task = PythonOperator(
    task_id='transform_multiple_weather_data',
    python_callable=transform_multiple_weather_data,
    dag=dag
)

# Load multiple cities data
load_multiple_task = PythonOperator(
    task_id='load_multiple_weather_data',
    python_callable=load_multiple_weather_data,
    dag=dag
)

# Set dependencies for multiple cities pipeline
extract_multiple_cities_task >> transform_multiple_task >> load_multiple_task
"""

# Set task dependencies
extract_task >> transform_task >> load_task >> data_quality_check >> generate_summary

# Add failure notification to all tasks
for task in [extract_task, transform_task, load_task, data_quality_check]:
    task >> failure_notification

# Add documentation for the DAG
dag.doc_md = """
# Daily Weather Data ETL Pipeline

This DAG implements a complete ETL (Extract, Transform, Load) pipeline for daily weather data collection.

## Pipeline Overview

The pipeline consists of the following stages:

1. **Extract**: Fetches current weather data from OpenWeather API
2. **Transform**: Cleans and structures the raw API response
3. **Load**: Stores the processed data in database and CSV file
4. **Quality Check**: Validates the loaded data
5. **Summary**: Generates execution summary
6. **Notifications**: Sends email alerts on failures

## Configuration

- **Schedule**: Daily at 6:00 AM UTC
- **Default City**: London (configurable)
- **Database**: SQLite (configurable to PostgreSQL)
- **Retry Policy**: 3 retries with 5-minute intervals
- **Email Notifications**: Enabled for failures

## Data Schema

The pipeline captures the following weather metrics:
- Temperature (°C)
- Humidity (%)
- Wind Speed (m/s)
- Pressure (hPa)
- Weather Description
- Visibility
- Sunrise/Sunset times

## Dependencies

- OpenWeather API key (required)
- Python packages: requests, pandas, sqlalchemy
- Database: SQLite or PostgreSQL

## Monitoring

- Check Airflow UI for task status
- Monitor email notifications for failures
- Review data quality check results
- Verify CSV and database outputs

## Customization

To modify the pipeline:
1. Update city list in extract task
2. Adjust schedule_interval for different frequency
3. Modify data transformation logic
4. Configure database connection settings
5. Update email notification recipients
"""
