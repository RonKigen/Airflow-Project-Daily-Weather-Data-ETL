# Daily Weather Data ETL with Apache Airflow

This project demonstrates a comprehensive ETL pipeline using Apache Airflow to extract, transform, and load daily weather data from WeatherAPI.com. The system is production-ready with Docker deployment, comprehensive testing, and real-time monitoring.

## 🌟 Project Overview

A complete weather data pipeline that:
- ✅ Extracts real-time weather data from 8 major cities
- ✅ Processes and validates data with robust error handling 
- ✅ Stores data in both database and CSV formats
- ✅ Runs on automated daily schedules
- ✅ Provides comprehensive monitoring and alerting
- ✅ Deployed using Docker for easy setup

## 📁 Project Structure

```
├── dags/
│   ├── weather_etl_dag.py          # Basic daily ETL DAG
│   └── advanced_weather_etl_dag.py # Multi-city ETL with monitoring
├── plugins/
│   └── weather_operators.py        # Custom Airflow operators
├── scripts/
│   ├── extract_weather.py          # WeatherAPI.com data extraction
│   ├── transform_weather.py        # Data cleaning and validation
│   └── load_weather.py             # Database and CSV storage
├── config/
│   └── config.py                   # Configuration management
├── data/
│   └── weather_data.csv            # Output data storage
├── sql/
│   └── create_tables.sql           # Database schema
├── tests/
│   ├── unit_tests.py               # Unit testing suite
│   └── test_*.py                   # Comprehensive test files
├── .vscode/
│   └── settings.json               # VS Code configuration
├── requirements.txt                # Python dependencies
├── docker-compose.yml              # Full Docker setup
├── docker-compose-simple.yml       # Simplified Docker deployment
├── .env                            # Environment variables
├── pyproject.toml                  # Python project configuration
├── test_pipeline.py                # Integration testing
└── TESTING.md                      # Testing documentation
```

## 🚀 Features

### Core ETL Pipeline
- **Extract**: Real-time weather data from WeatherAPI.com for 8 major cities
- **Transform**: Advanced data cleaning, validation, and quality checks
- **Load**: Dual storage in PostgreSQL/SQLite database and CSV files
- **Scheduling**: Automated daily execution at 6 AM UTC

### Advanced Capabilities
- **Multi-City Support**: London, New York, Tokyo, Sydney, Paris, Berlin, Toronto, Mumbai
- **Custom Operators**: Specialized Airflow operators for weather data processing
- **Error Handling**: Comprehensive retry mechanisms and failure notifications
- **Data Validation**: Built-in data quality checks and anomaly detection
- **Monitoring**: Real-time dashboard and email alerts
- **Testing**: Complete unit and integration test suites

### Production Ready
- **Docker Deployment**: One-command setup with docker-compose
- **Environment Management**: Secure configuration with environment variables
- **Logging**: Detailed logging for debugging and monitoring
- **VS Code Integration**: Optimized development environment setup

## 🛠️ Quick Start with Docker

### Prerequisites
- Docker and Docker Compose installed
- WeatherAPI.com API key (free at https://weatherapi.com)

### 1. Clone and Setup
```bash
git clone <repository-url>
cd "Airflow Project – Daily Weather Data ETL"
```

### 2. Configure Environment
Update `.env` file with your API key:
```bash
WEATHERAPI_API_KEY=your_api_key_here
```

### 3. Deploy with Docker
```bash
# Set environment variable (Windows)
$env:AIRFLOW_UID="50000"

# Start the system
docker-compose -f docker-compose-simple.yml up
```

### 4. Access Airflow UI
- Open: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

### 5. Enable DAGs
- Turn on `weather_etl_dag` for basic functionality
- Turn on `advanced_weather_etl_dag` for multi-city processing

## 🔧 Manual Setup (Alternative)

If you prefer to run without Docker:

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
# Copy and edit environment file
cp .env.example .env
# Add your WeatherAPI.com API key
```

### 3. Initialize Airflow
```bash
# Set Airflow home
export AIRFLOW_HOME=$(pwd)

# Initialize database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 4. Start Airflow Services
```bash
# Terminal 1: Start webserver
airflow webserver --port 8080

# Terminal 2: Start scheduler  
airflow scheduler
```

## 🧪 Testing

### Quick Tests
```bash
# Run unit tests
python tests/unit_tests.py

# Run integration tests
python test_pipeline.py
```

### Comprehensive Testing
```bash
# Install test dependencies
pip install pytest pytest-cov requests-mock

# Run full test suite
pytest tests/ -v --cov=scripts
```

## 📊 Data Schema

### Weather Data Table
| Field | Type | Description |
|-------|------|-------------|
| id | INTEGER | Primary key |
| timestamp | DATETIME | Data collection time |
| city | VARCHAR(100) | City name |
| temperature | FLOAT | Temperature in Celsius |
| feels_like | FLOAT | Feels like temperature |
| humidity | INTEGER | Humidity percentage |
| wind_speed | FLOAT | Wind speed in m/s |
| wind_direction | VARCHAR(10) | Wind direction |
| pressure | FLOAT | Atmospheric pressure in hPa |
| visibility | FLOAT | Visibility in km |
| uv_index | FLOAT | UV index |
| weather_description | VARCHAR(255) | Weather condition |
| weather_icon | VARCHAR(50) | Weather icon code |

## 🌍 Supported Cities

The pipeline collects data for these 8 major cities:
- 🇬🇧 London, UK
- 🇺🇸 New York, USA  
- 🇯🇵 Tokyo, Japan
- 🇦🇺 Sydney, Australia
- 🇫🇷 Paris, France
- 🇩🇪 Berlin, Germany
- 🇨🇦 Toronto, Canada
- 🇮🇳 Mumbai, India

## ⚙️ Configuration

### Environment Variables
```bash
# WeatherAPI.com Configuration
WEATHERAPI_API_KEY=your_api_key_here

# Database Configuration  
DB_TYPE=sqlite  # or postgresql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=weather_db
DB_USER=your_username
DB_PASSWORD=your_password

# Default Settings
DEFAULT_CITY=London
DATA_OUTPUT_PATH=./data/weather_data.csv

# Airflow Configuration (for Docker)
AIRFLOW_UID=50000
AIRFLOW_GID=0
```

### DAG Configuration
- **Basic DAG**: `weather_etl_dag.py` - Single city, simple processing
- **Advanced DAG**: `advanced_weather_etl_dag.py` - Multi-city with monitoring
- **Schedule**: Daily at 6:00 AM UTC
- **Retry**: 3 attempts with 5-minute delays
- **Timeout**: 10 minutes per task

## 📋 API Requirements

### WeatherAPI.com
- **Free Tier**: 1,000,000 calls/month
- **Endpoint**: `http://api.weatherapi.com/v1/current.json`
- **Required**: API key registration
- **Rate Limits**: Generous limits for daily ETL

### Sample API Response
```json
{
  "location": {
    "name": "London",
    "country": "United Kingdom",
    "localtime": "2025-07-25 14:30"
  },
  "current": {
    "temp_c": 19.1,
    "feelslike_c": 18.5,
    "humidity": 73,
    "wind_kph": 11.2,
    "pressure_mb": 1013.0,
    "vis_km": 10.0,
    "uv": 4.0,
    "condition": {
      "text": "Partly cloudy",
      "icon": "//cdn.weatherapi.com/weather/64x64/day/116.png"
    }
  }
}
```

## 🐳 Docker Components

### Services
- **airflow-standalone**: Complete Airflow instance with web UI and scheduler
- **postgres**: PostgreSQL database for Airflow metadata
- **volumes**: Persistent storage for logs, DAGs, and data

### Docker Commands
```bash
# Start services
docker-compose -f docker-compose-simple.yml up -d

# Check status
docker-compose -f docker-compose-simple.yml ps

# View logs
docker-compose -f docker-compose-simple.yml logs

# Stop services
docker-compose -f docker-compose-simple.yml down

# Rebuild after changes
docker-compose -f docker-compose-simple.yml up --build
```

## 📈 Monitoring & Alerts

### Airflow UI Features
- **DAG Overview**: Visual pipeline status and execution history
- **Task Logs**: Detailed execution logs for debugging
- **Gantt Chart**: Task duration and dependency visualization  
- **Tree View**: Historical run status across time
- **Graph View**: DAG structure and task relationships

### Email Notifications
Configure in DAG files for:
- Task failures
- Retry attempts
- SLA breaches
- Data quality alerts

### Custom Metrics
- API response times
- Data quality scores
- Processing durations
- Error rates

## 🛠️ Development

### VS Code Setup
The project includes optimized VS Code settings:
- Python testing configuration
- Jest disabled for Python projects  
- Integrated debugging support
- Code formatting and linting

### Adding New Cities
1. Update `CITIES` list in `advanced_weather_etl_dag.py`
2. Test with `python test_pipeline.py`
3. Deploy changes

### Custom Operators
Extend functionality by creating new operators in `plugins/weather_operators.py`:
- Data validation operators
- External API integrators
- Custom notification handlers

## 🔍 Troubleshooting

### Common Issues

#### Docker Startup Problems
```bash
# Set AIRFLOW_UID environment variable
$env:AIRFLOW_UID="50000"  # Windows
export AIRFLOW_UID=50000   # Linux/Mac

# Clean restart
docker-compose -f docker-compose-simple.yml down
docker-compose -f docker-compose-simple.yml up
```

#### API Connection Issues
- Verify API key in `.env` file
- Check WeatherAPI.com quota limits
- Test connection: `python test_pipeline.py`

#### Database Connection Problems
- Ensure PostgreSQL is running
- Check connection string in `.env`
- Verify database exists and permissions

#### DAG Not Appearing
- Check DAG syntax: `airflow dags list`
- Verify file placement in `dags/` folder
- Check Airflow logs for import errors

### Logs and Debugging
```bash
# Airflow logs
docker-compose -f docker-compose-simple.yml logs airflow-standalone

# Database logs  
docker-compose -f docker-compose-simple.yml logs postgres

# Task-specific logs (in Airflow UI)
# Navigate to: DAGs → [dag_name] → [task] → Logs
```

## 🤝 Contributing

### Development Workflow
1. Fork the repository
2. Create feature branch: `git checkout -b feature/new-feature`
3. Make changes and test: `python test_pipeline.py`
4. Commit changes: `git commit -m "Add new feature"`
5. Push to branch: `git push origin feature/new-feature`
6. Create Pull Request

### Testing Requirements
- All new features must have unit tests
- Integration tests for API changes
- Docker deployment verification
- Documentation updates

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **WeatherAPI.com** for providing reliable weather data API
- **Apache Airflow** community for the robust orchestration platform
- **Docker** for containerization support

## 📧 Support

For questions, issues, or contributions:
- Create an issue on GitHub
- Check existing documentation in `TESTING.md`
- Review Airflow logs for debugging information

---

**Happy Weather Data Processing! 🌤️📊**
