@echo off
echo 🌤️  Setting up Daily Weather Data ETL Project...

REM Create necessary directories
echo 📁 Creating project directories...
if not exist logs mkdir logs
if not exist plugins mkdir plugins
if not exist data mkdir data

REM Copy environment file
if not exist .env (
    echo 🔧 Creating environment file...
    copy .env.example .env
    echo ⚠️  Please update .env file with your OpenWeather API key!
)

REM Install Python dependencies
where python >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    echo 📦 Installing Python dependencies...
    pip install -r requirements.txt
) else (
    echo ⚠️  Python not found. Please install dependencies manually:
    echo     pip install -r requirements.txt
)

REM Initialize SQLite database
echo 🗄️  Setting up SQLite database...
python -c "import sqlite3; import os; conn = sqlite3.connect('weather_data.db'); sql_script = open('sql/create_tables.sql', 'r').read(); sql_script = sql_script.replace('SERIAL PRIMARY KEY', 'INTEGER PRIMARY KEY AUTOINCREMENT'); sql_script = sql_script.replace('TIMESTAMP DEFAULT CURRENT_TIMESTAMP', 'DATETIME DEFAULT CURRENT_TIMESTAMP'); conn.executescript(sql_script); conn.close(); print('✅ SQLite database created: weather_data.db')"

echo.
echo ✅ Setup completed successfully!
echo.
echo 📋 Next steps:
echo 1. Update .env file with your OpenWeather API key
echo 2. To run with Docker: docker-compose up -d
echo 3. To run locally: Set up Airflow and start webserver/scheduler
echo 4. Access Airflow UI at http://localhost:8080
echo 5. Default credentials: airflow/airflow
echo.
echo 🚀 Happy data engineering!
pause
