# 🌤️ Daily Weather Data ETL - Project Documentation

## Table of Contents
1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Setup Guide](#setup-guide)
4. [API Configuration](#api-configuration)
5. [Usage Examples](#usage-examples)
6. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
7. [Customization](#customization)
8. [Performance Optimization](#performance-optimization)

---

## Project Overview

This project implements a production-ready ETL (Extract, Transform, Load) pipeline using Apache Airflow to collect and process daily weather data from the WeatherAPI.com service. The pipeline demonstrates best practices in data engineering, workflow orchestration, and automated data processing.

### 🎯 Key Features

- **Automated Data Collection**: Scheduled extraction from WeatherAPI.com
- **Data Quality Assurance**: Comprehensive validation and cleaning
- **Multi-Format Storage**: SQLite/PostgreSQL database + CSV files
- **Real-time Monitoring**: Email alerts and quality checks
- **Scalable Architecture**: Support for multiple cities and data sources
- **Custom Operators**: Reusable Airflow components
- **Historical Analysis**: Trend reporting and data analysis

### 🏗️ Technical Stack

- **Orchestration**: Apache Airflow 2.8.1
- **Language**: Python 3.8+
- **Database**: SQLite (default) / PostgreSQL
- **Data Processing**: Pandas, SQLAlchemy
- **API Integration**: Requests library
- **Containerization**: Docker & Docker Compose
- **Monitoring**: Email notifications, Airflow UI

---

## Architecture

### 📊 Data Flow Diagram

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  WeatherAPI.com │    │   Apache         │    │  Data Storage   │
│  - Current      │───▶│   Airflow        │───▶│  - SQLite/      │
│  - Multi-city   │    │   - Scheduler    │    │    PostgreSQL   │
│  - Real-time    │    │   - Webserver    │    │  - CSV Files    │
└─────────────────┘    │   - Workers      │    │  - Reports      │
                       └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │  Notifications   │
                       │  - Email Alerts  │
                       │  - Quality       │
                       │    Reports       │
                       └──────────────────┘
```

### 🔄 ETL Pipeline Stages

1. **Extract**: API calls to WeatherAPI.com service
2. **Transform**: Data cleaning, validation, and formatting
3. **Load**: Storage in database and file systems
4. **Monitor**: Quality checks and alerting
5. **Report**: Statistics and trend analysis

### 📁 Project Structure

```
Airflow Project – Daily Weather Data ETL/
├── 📂 dags/                          # Airflow DAG definitions
│   ├── weather_etl_dag.py           # Basic ETL pipeline
│   └── advanced_weather_etl_dag.py  # Advanced multi-city pipeline
├── 📂 scripts/                       # Core ETL logic
│   ├── extract_weather.py           # Data extraction functions
│   ├── transform_weather.py         # Data transformation logic
│   └── load_weather.py              # Data loading operations
├── 📂 plugins/                       # Custom Airflow operators
│   └── weather_operators.py         # Reusable weather operators
├── 📂 config/                        # Configuration management
│   └── config.py                    # Environment and settings
├── 📂 sql/                          # Database schemas
│   └── create_tables.sql            # Table creation scripts
├── 📂 data/                         # Output data storage
│   └── weather_data.csv             # Processed weather data
├── 📂 logs/                         # Airflow logs (auto-generated)
├── 📄 requirements.txt              # Python dependencies
├── 📄 docker-compose.yml            # Docker orchestration
├── 📄 setup.sh / setup.bat          # Setup scripts
├── 📄 test_pipeline.py              # Testing utilities
└── 📄 .env.example                  # Environment template
```

---

## Setup Guide

### 🔧 Prerequisites

- Python 3.8 or higher
- Docker & Docker Compose (for containerized setup)
- WeatherAPI.com API key (free tier: 1 million calls/month)
- 4GB RAM minimum for Docker setup

### 📦 Installation Options

#### Option 1: Docker Setup (Recommended)

1. **Clone and Configure**:
   ```bash
   cd "Airflow Project – Daily Weather Data ETL"
   cp .env.example .env
   # Edit .env with your API key
   ```

2. **Start Services**:
   ```bash
   docker-compose up -d
   ```

3. **Access Airflow**:
   - UI: http://localhost:8080
   - Credentials: airflow/airflow

#### Option 2: Local Installation

1. **Setup Environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Run Setup Script**:
   ```bash
   chmod +x setup.sh
   ./setup.sh  # Windows: setup.bat
   ```

3. **Initialize Airflow**:
   ```bash
   export AIRFLOW_HOME=$(pwd)
   airflow db init
   airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
   ```

4. **Start Airflow**:
   ```bash
   airflow webserver --port 8080 &
   airflow scheduler &
   ```

### 🧪 Testing Setup

Run the test pipeline to verify installation:

```bash
python test_pipeline.py
```

---

## API Configuration

### 🔑 WeatherAPI.com API Setup

1. **Get API Key**:
   - Visit [WeatherAPI.com](https://www.weatherapi.com/)
   - Sign up for free account
   - Generate API key

2. **Configure Environment**:
   ```bash
   # Update .env file
   WEATHERAPI_API_KEY=your_actual_api_key_here
   DEFAULT_CITY=London
   ```

3. **API Limits** (Free Tier):
   - 1,000,000 calls/month
   - Current weather data included

### 🌍 Supported Cities

The pipeline supports any city name recognized by WeatherAPI.com:

**Default Cities**:
- London, UK
- New York, US
- Tokyo, JP
- Sydney, AU
- Mumbai, IN
- Berlin, DE
- Toronto, CA
- São Paulo, BR

**Add Custom Cities**:
```python
# In advanced_weather_etl_dag.py
CITIES = ['Your City', 'Another City', 'Third City']
```

---

## Usage Examples

### 🚀 Running DAGs

#### Basic Weather ETL
```python
# Runs daily at 6 AM UTC
# Single city (London by default)
# Basic ETL operations
```

#### Advanced Multi-City ETL
```python
# Runs every 6 hours
# Multiple cities simultaneously
# Advanced monitoring and reporting
```

### 📊 Data Access Examples

#### Query Database
```python
from scripts.load_weather import get_weather_history
import pandas as pd

# Get last 7 days of data
df = get_weather_history(days=7)
print(df.head())

# Get data for specific city
df_london = get_weather_history(city='London', days=7)
```

#### Analyze CSV Data
```python
import pandas as pd

df = pd.read_csv('data/weather_data.csv')
print(f"Average temperature: {df['temperature'].mean():.2f}°C")
print(f"Cities monitored: {df['city'].unique()}")
```

### 📈 Custom Analytics

```python
# Temperature trend analysis
df['date'] = pd.to_datetime(df['timestamp']).dt.date
daily_avg = df.groupby(['date', 'city'])['temperature'].mean()

# Weather alerts
hot_days = df[df['temperature'] > 30]
humid_days = df[df['humidity'] > 90]
```

---

## Monitoring & Troubleshooting

### 📊 Monitoring Dashboard

**Airflow UI Features**:
- Task status and history
- Log viewing and analysis
- DAG dependency graphs
- Performance metrics

**Key Metrics to Monitor**:
- Task success/failure rates
- Execution duration
- Data quality check results
- API response times

### 🔍 Common Issues & Solutions

#### 1. API Key Issues
```
Error: "Invalid API key"
Solution: Check .env file and Airflow Variables
```

#### 2. Database Connection
```
Error: "Database connection failed"
Solution: Verify database configuration and permissions
```

#### 3. Memory Issues
```
Error: Docker containers crashing
Solution: Increase Docker memory allocation (4GB minimum)
```

#### 4. Network Timeouts
```
Error: "Request timeout"
Solution: Check internet connection and API limits
```

### 📧 Alert Configuration

**Email Settings**:
```python
# In DAG configuration
default_args = {
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['your-email@example.com']
}
```

**SMTP Configuration** (airflow.cfg):
```ini
[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = your-email@gmail.com
smtp_password = your-app-password
smtp_port = 587
smtp_mail_from = your-email@gmail.com
```

---

## Customization

### 🎛️ Configuration Options

#### Scheduling
```python
# Modify schedule_interval in DAG definition
schedule_interval='0 6 * * *'    # Daily at 6 AM
schedule_interval='0 */6 * * *'  # Every 6 hours
schedule_interval='@hourly'      # Every hour
```

#### Data Retention
```python
# In load_weather.py
cleanup_old_data(days_to_keep=30)  # Keep 30 days of data
```

#### Quality Thresholds
```python
# In weather_operators.py
temperature_range=(-50, 60)  # Valid temperature range
humidity_range=(0, 100)      # Valid humidity range
pressure_range=(870, 1085)   # Valid pressure range
```

### 🔌 Adding New Data Sources

1. **Create New Extractor**:
```python
def extract_from_new_api(city, **context):
    # Your extraction logic
    pass
```

2. **Update DAG**:
```python
new_extract_task = PythonOperator(
    task_id='extract_new_source',
    python_callable=extract_from_new_api
)
```

### 📊 Custom Transformations

```python
def custom_transform(data):
    # Add heat index calculation
    data['heat_index'] = calculate_heat_index(
        data['temperature'], 
        data['humidity']
    )
    
    # Add comfort level
    data['comfort_level'] = categorize_comfort(
        data['temperature'],
        data['humidity']
    )
    
    return data
```

---

## Performance Optimization

### 🚀 Scaling Strategies

#### 1. Parallel Processing
```python
# Process multiple cities in parallel
from airflow.operators.python import BranchPythonOperator
from concurrent.futures import ThreadPoolExecutor

def parallel_extraction(cities):
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(extract_weather_data, cities))
    return results
```

#### 2. Database Optimization
```sql
-- Add indexes for better query performance
CREATE INDEX idx_weather_city_timestamp ON weather_data(city, timestamp);
CREATE INDEX idx_weather_temperature ON weather_data(temperature);
```

#### 3. Caching Strategy
```python
from functools import lru_cache

@lru_cache(maxsize=100)
def cached_api_call(city, timestamp):
    # Cache API responses for same city/time
    pass
```

### 📈 Monitoring Performance

#### Resource Usage
```bash
# Monitor Docker containers
docker stats

# Check Airflow task performance
# View in Airflow UI: Admin > Task Duration
```

#### API Usage Optimization
```python
# Batch requests where possible
# Implement exponential backoff
# Monitor rate limits
```

### 🔄 Data Pipeline Optimization

1. **Batch Processing**: Group operations by city/time
2. **Incremental Loading**: Only process new/changed data
3. **Compression**: Use compressed CSV formats
4. **Partitioning**: Organize data by date/city

---

## 📚 Additional Resources

### Documentation Links
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [OpenWeather API Docs](https://openweathermap.org/api)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)

### Best Practices
- Use environment variables for sensitive data
- Implement comprehensive logging
- Set up proper monitoring and alerting
- Regular backup of critical data
- Document all customizations

### Community Resources
- [Airflow Slack Community](https://apache-airflow-slack.herokuapp.com/)
- [Stack Overflow - Airflow](https://stackoverflow.com/questions/tagged/apache-airflow)
- [GitHub Issues](https://github.com/apache/airflow/issues)

---

**🎉 Congratulations!** You now have a production-ready weather data ETL pipeline. Happy data engineering!
