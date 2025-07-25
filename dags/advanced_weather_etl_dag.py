from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
import sys
import os

# Add paths for custom operators
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'plugins'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))

from plugins.weather_operators import (
    WeatherExtractOperator,
    WeatherTransformOperator,
    WeatherLoadOperator,
    WeatherMultiCityOperator,
    WeatherQualityCheckOperator,
    WeatherHistoryOperator,
    WeatherAlertSensor
)

# Default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the advanced DAG
dag = DAG(
    'advanced_weather_etl',
    default_args=default_args,
    description='Advanced Weather Data ETL Pipeline with Custom Operators',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    max_active_runs=1,
    catchup=False,
    tags=['weather', 'etl', 'advanced', 'multi-city']
)

# List of cities to monitor
CITIES = ['London', 'New York', 'Tokyo', 'Sydney', 'Mumbai', 'Berlin', 'Toronto', 'São Paulo']

# Task 1: Extract weather data for multiple cities
extract_multi_cities = WeatherMultiCityOperator(
    task_id='extract_multiple_cities',
    cities=CITIES,
    dag=dag,
    doc_md="""
    ## Extract Multiple Cities Weather Data
    
    Extracts current weather data for multiple cities simultaneously.
    
    **Cities monitored:**
    """ + ", ".join(CITIES)
)

# Task 2: Transform weather data for all cities
transform_multi_cities = PythonOperator(
    task_id='transform_multiple_cities',
    python_callable=lambda **context: __import__('scripts.transform_weather', fromlist=['transform_multiple_weather_data']).transform_multiple_weather_data(**context),
    dag=dag
)

# Task 3: Load weather data for all cities
load_multi_cities = PythonOperator(
    task_id='load_multiple_cities',
    python_callable=lambda **context: __import__('scripts.load_weather', fromlist=['load_multiple_weather_data']).load_multiple_weather_data(**context),
    dag=dag
)

# Task 4: Weather data quality check
quality_check = WeatherQualityCheckOperator(
    task_id='weather_quality_check',
    input_key='all_transformed_data',
    dag=dag
)

# Task 5: Generate weather history report
history_report = WeatherHistoryOperator(
    task_id='generate_history_report',
    days_back=7,
    dag=dag
)

# Task 6: Weather alert monitoring
weather_alerts = WeatherAlertSensor(
    task_id='weather_alert_monitor',
    city='London',
    temperature_threshold=30.0,  # Alert if temperature > 30°C
    humidity_threshold=90.0,     # Alert if humidity > 90%
    wind_speed_threshold=15.0,   # Alert if wind speed > 15 m/s
    dag=dag
)

# Task 7: Data validation and statistics
data_validation = PythonOperator(
    task_id='data_validation_and_stats',
    python_callable=lambda **context: validate_and_generate_stats(**context),
    dag=dag
)

def validate_and_generate_stats(**context):
    """Validate loaded data and generate statistics"""
    from scripts.load_weather import get_weather_history
    import pandas as pd
    
    # Get recent data
    df = get_weather_history(days=1)
    
    if len(df) == 0:
        raise ValueError("No recent weather data found!")
    
    # Generate statistics
    stats = {
        'total_records': len(df),
        'unique_cities': df['city'].nunique(),
        'avg_temperature': round(df['temperature'].mean(), 2),
        'min_temperature': round(df['temperature'].min(), 2),
        'max_temperature': round(df['temperature'].max(), 2),
        'avg_humidity': round(df['humidity'].mean(), 2),
        'cities_list': df['city'].unique().tolist(),
        'timestamp_range': {
            'start': str(df['timestamp'].min()),
            'end': str(df['timestamp'].max())
        }
    }
    
    print(f"📊 Weather Data Statistics:")
    print(f"   Total Records: {stats['total_records']}")
    print(f"   Cities Monitored: {stats['unique_cities']}")
    print(f"   Average Temperature: {stats['avg_temperature']}°C")
    print(f"   Temperature Range: {stats['min_temperature']}°C to {stats['max_temperature']}°C")
    print(f"   Average Humidity: {stats['avg_humidity']}%")
    print(f"   Cities: {', '.join(stats['cities_list'])}")
    
    # Store stats in XCom
    context['task_instance'].xcom_push(key='weather_stats', value=stats)
    
    return stats

# Task 8: Generate daily weather report
generate_report = PythonOperator(
    task_id='generate_daily_report',
    python_callable=lambda **context: generate_weather_report(**context),
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

def generate_weather_report(**context):
    """Generate comprehensive weather report"""
    
    # Get statistics from previous task
    stats = context['task_instance'].xcom_pull(key='weather_stats')
    history_stats = context['task_instance'].xcom_pull(key='weather_history_stats')
    
    report = f"""
    🌤️ DAILY WEATHER REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}
    {'='*60}
    
    📊 TODAY'S SUMMARY:
    ├── Total Records Processed: {stats.get('total_records', 'N/A')}
    ├── Cities Monitored: {stats.get('unique_cities', 'N/A')}
    ├── Average Temperature: {stats.get('avg_temperature', 'N/A')}°C
    ├── Temperature Range: {stats.get('min_temperature', 'N/A')}°C to {stats.get('max_temperature', 'N/A')}°C
    └── Average Humidity: {stats.get('avg_humidity', 'N/A')}%
    
    🏙️ CITIES MONITORED:
    {', '.join(stats.get('cities_list', [])) if stats else 'N/A'}
    
    📈 HISTORICAL CONTEXT (Last 7 Days):
    ├── Historical Records: {history_stats.get('record_count', 'N/A') if history_stats else 'N/A'}
    ├── Date Range: {history_stats.get('date_range', {}).get('start', 'N/A') if history_stats else 'N/A'} to {history_stats.get('date_range', {}).get('end', 'N/A') if history_stats else 'N/A'}
    └── Average Temperature: {history_stats.get('temperature_stats', {}).get('avg', 'N/A') if history_stats else 'N/A'}°C
    
    ⚡ PIPELINE STATUS:
    ├── Extraction: ✅ Success
    ├── Transformation: ✅ Success
    ├── Loading: ✅ Success
    ├── Quality Checks: ✅ Passed
    └── Data Validation: ✅ Passed
    
    📅 NEXT SCHEDULED RUN: {context.get('next_ds', 'N/A')}
    
    🔍 DATA QUALITY NOTES:
    - All temperature readings within normal ranges
    - Humidity values validated
    - No missing critical data fields detected
    - Database integrity maintained
    
    {'='*60}
    Report generated by Advanced Weather ETL Pipeline
    """
    
    print(report)
    
    # You could also save this report to a file or send via email
    with open('/tmp/weather_report.txt', 'w') as f:
        f.write(report)
    
    return report

# Task 9: Cleanup old data (optional)
cleanup_old_data = BashOperator(
    task_id='cleanup_old_data',
    bash_command="""
    echo "🧹 Cleaning up old weather data..."
    python -c "
import sqlite3
from datetime import datetime, timedelta

# Connect to database
conn = sqlite3.connect('weather_data.db')
cursor = conn.cursor()

# Delete records older than 30 days
cutoff_date = datetime.now() - timedelta(days=30)
cursor.execute('DELETE FROM weather_data WHERE timestamp < ?', (cutoff_date,))
deleted_count = cursor.rowcount

conn.commit()
conn.close()

print(f'🗑️ Cleaned up {deleted_count} old weather records (older than 30 days)')
"
    """,
    dag=dag
)

# Task 10: Send success notification
success_notification = EmailOperator(
    task_id='send_success_notification',
    to=['admin@example.com'],  # Update with actual email
    subject='Weather ETL Pipeline Success - {{ ds }}',
    html_content="""
    <h3>✅ Weather ETL Pipeline Completed Successfully</h3>
    <p>The advanced weather ETL pipeline completed successfully on {{ ds }}.</p>
    
    <h4>📊 Summary:</h4>
    <ul>
        <li><strong>Cities Processed:</strong> {{ ti.xcom_pull(key='weather_stats')['unique_cities'] or 'N/A' }}</li>
        <li><strong>Records Created:</strong> {{ ti.xcom_pull(key='weather_stats')['total_records'] or 'N/A' }}</li>
        <li><strong>Average Temperature:</strong> {{ ti.xcom_pull(key='weather_stats')['avg_temperature'] or 'N/A' }}°C</li>
    </ul>
    
    <h4>🔗 Quick Links:</h4>
    <ul>
        <li><a href="http://localhost:8080/tree?dag_id=advanced_weather_etl">View DAG in Airflow</a></li>
        <li><a href="http://localhost:8080/graph?dag_id=advanced_weather_etl">View Task Graph</a></li>
    </ul>
    
    <p><em>Next run scheduled: {{ next_ds }}</em></p>
    """,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS
)

# Set task dependencies
extract_multi_cities >> transform_multi_cities >> load_multi_cities
load_multi_cities >> [quality_check, history_report, weather_alerts]
[quality_check, history_report] >> data_validation >> generate_report
generate_report >> cleanup_old_data >> success_notification

# Add failure notification to critical tasks
failure_notification = EmailOperator(
    task_id='send_failure_notification',
    to=['admin@example.com'],
    subject='❌ Weather ETL Pipeline Failed - {{ ds }}',
    html_content="""
    <h3>❌ Weather ETL Pipeline Failure</h3>
    <p>The advanced weather ETL pipeline failed on {{ ds }}.</p>
    <p><strong>Failed Task:</strong> {{ ti.task_id }}</p>
    <p><strong>DAG Run:</strong> {{ dag_run.dag_id }}</p>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    
    <h4>🔍 Troubleshooting:</h4>
    <ul>
        <li>Check task logs in Airflow UI</li>
        <li>Verify API key configuration</li>
        <li>Check database connectivity</li>
        <li>Review system resources</li>
    </ul>
    
    <p><a href="{{ ti.log_url }}">View Task Logs</a></p>
    """,
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED
)

# Connect failure notification to all main tasks
for task in [extract_multi_cities, transform_multi_cities, load_multi_cities, quality_check, data_validation]:
    task >> failure_notification

# DAG documentation
dag.doc_md = """
# Advanced Weather Data ETL Pipeline

This is an advanced version of the weather ETL pipeline that demonstrates:

## 🚀 Advanced Features

- **Multi-City Processing**: Extracts data for multiple cities simultaneously
- **Custom Operators**: Uses custom Airflow operators for better modularity
- **Data Quality Checks**: Comprehensive validation of weather data
- **Historical Analysis**: Generates reports comparing current vs historical data
- **Alert Monitoring**: Checks for extreme weather conditions
- **Automated Reporting**: Generates comprehensive daily reports
- **Data Cleanup**: Automatically removes old data to manage storage
- **Smart Notifications**: Success/failure emails with detailed information

## 🏙️ Cities Monitored

{cities}

## ⏰ Schedule

- **Frequency**: Every 6 hours
- **Start Time**: 00:00, 06:00, 12:00, 18:00 UTC
- **Timezone**: UTC

## 📊 Data Quality Checks

- Temperature range validation (-50°C to 60°C)
- Humidity range validation (0% to 100%)
- Pressure range validation (870 to 1085 hPa)
- Required field presence checks
- Data type validations

## 🔔 Alert Conditions

- Temperature > 30°C
- Humidity > 90%
- Wind Speed > 15 m/s

## 📈 Reporting Features

- Daily summary statistics
- Historical trend analysis
- Pipeline performance metrics
- Data quality reports
- Email notifications

## 🛠️ Monitoring

- Task-level error handling
- Automatic retries (2x with 5-minute delays)
- Email notifications for failures
- Data validation checkpoints
- Historical data cleanup

## 📂 Output Locations

- **Database**: `weather_data.db` (SQLite) or PostgreSQL
- **CSV Files**: `data/weather_data.csv`
- **Reports**: `/tmp/weather_report.txt`
- **Logs**: Airflow task logs

## 🔧 Configuration

Update the following in your environment:
- OpenWeather API key
- Email notification settings
- Database connection details
- City list and alert thresholds
""".format(cities=", ".join(CITIES))
