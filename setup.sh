#!/bin/bash

# Weather ETL Project Setup Script

echo "🌤️  Setting up Daily Weather Data ETL Project..."

# Create necessary directories
echo "📁 Creating project directories..."
mkdir -p logs plugins data

# Copy environment file
if [ ! -f .env ]; then
    echo "🔧 Creating environment file..."
    cp .env.example .env
    echo "⚠️  Please update .env file with your OpenWeather API key!"
fi

# Install Python dependencies (if virtual environment is available)
if command -v python &> /dev/null; then
    echo "📦 Installing Python dependencies..."
    pip install -r requirements.txt
else
    echo "⚠️  Python not found. Please install dependencies manually:"
    echo "    pip install -r requirements.txt"
fi

# Initialize SQLite database
echo "🗄️  Setting up SQLite database..."
python -c "
import sqlite3
import os

# Create database file
db_path = 'weather_data.db'
conn = sqlite3.connect(db_path)

# Create tables
with open('sql/create_tables.sql', 'r') as f:
    sql_script = f.read()
    # Replace PostgreSQL-specific syntax for SQLite
    sql_script = sql_script.replace('SERIAL PRIMARY KEY', 'INTEGER PRIMARY KEY AUTOINCREMENT')
    sql_script = sql_script.replace('TIMESTAMP DEFAULT CURRENT_TIMESTAMP', 'DATETIME DEFAULT CURRENT_TIMESTAMP')
    conn.executescript(sql_script)

conn.close()
print(f'✅ SQLite database created: {db_path}')
"

# Set proper permissions for Airflow (Linux/Mac)
if [ "$(uname)" != "MINGW64_NT"* ]; then
    echo "🔐 Setting directory permissions..."
    chmod -R 755 dags scripts config
fi

echo ""
echo "✅ Setup completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Update .env file with your OpenWeather API key"
echo "2. To run with Docker: docker-compose up -d"
echo "3. To run locally: Set up Airflow and start webserver/scheduler"
echo "4. Access Airflow UI at http://localhost:8080"
echo "5. Default credentials: airflow/airflow"
echo ""
echo "🚀 Happy data engineering!"
