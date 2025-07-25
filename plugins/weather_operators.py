"""
Custom Airflow Operators for Weather ETL Pipeline
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

import sys
import os

# Add scripts to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))


class WeatherExtractOperator(BaseOperator):
    """
    Custom operator for extracting weather data from WeatherAPI.com
    """
    
    template_fields = ['city', 'api_key']
    ui_color = '#89CDF1'
    
    @apply_defaults
    def __init__(
        self,
        city: str = 'London',
        api_key: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.city = city
        self.api_key = api_key
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute weather data extraction"""
        from scripts.extract_weather import extract_weather_data
        
        self.log.info(f"Extracting weather data for {self.city}")
        
        try:
            weather_data = extract_weather_data(
                city=self.city,
                **context
            )
            
            self.log.info(f"Successfully extracted weather data for {self.city}")
            return weather_data
            
        except Exception as e:
            self.log.error(f"Failed to extract weather data: {str(e)}")
            raise AirflowException(f"Weather extraction failed: {str(e)}")


class WeatherTransformOperator(BaseOperator):
    """
    Custom operator for transforming weather data
    """
    
    template_fields = ['input_key']
    ui_color = '#F4A460'
    
    @apply_defaults
    def __init__(
        self,
        input_key: str = 'raw_weather_data',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_key = input_key
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute weather data transformation"""
        from scripts.transform_weather import transform_weather_data
        
        self.log.info("Starting weather data transformation")
        
        try:
            # Get raw data from previous task
            raw_data = context['task_instance'].xcom_pull(key=self.input_key)
            
            if not raw_data:
                raise AirflowException(f"No data found with key: {self.input_key}")
            
            transformed_data = transform_weather_data(
                raw_data=raw_data,
                **context
            )
            
            self.log.info("Successfully transformed weather data")
            return transformed_data
            
        except Exception as e:
            self.log.error(f"Failed to transform weather data: {str(e)}")
            raise AirflowException(f"Weather transformation failed: {str(e)}")


class WeatherLoadOperator(BaseOperator):
    """
    Custom operator for loading weather data to database and file
    """
    
    template_fields = ['input_key', 'output_path']
    ui_color = '#90EE90'
    
    @apply_defaults
    def __init__(
        self,
        input_key: str = 'transformed_weather_data',
        output_path: Optional[str] = None,
        load_to_db: bool = True,
        load_to_csv: bool = True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_key = input_key
        self.output_path = output_path
        self.load_to_db = load_to_db
        self.load_to_csv = load_to_csv
    
    def execute(self, context: Dict[str, Any]) -> bool:
        """Execute weather data loading"""
        from scripts.load_weather import load_to_database, load_to_csv
        
        self.log.info("Starting weather data loading")
        
        try:
            # Get transformed data from previous task
            transformed_data = context['task_instance'].xcom_pull(key=self.input_key)
            
            if not transformed_data:
                raise AirflowException(f"No data found with key: {self.input_key}")
            
            # Load to database
            if self.load_to_db:
                load_to_database(transformed_data)
                self.log.info("Successfully loaded data to database")
            
            # Load to CSV
            if self.load_to_csv:
                load_to_csv(transformed_data)
                self.log.info("Successfully loaded data to CSV")
            
            self.log.info("Weather data loading completed successfully")
            return True
            
        except Exception as e:
            self.log.error(f"Failed to load weather data: {str(e)}")
            raise AirflowException(f"Weather loading failed: {str(e)}")


class WeatherMultiCityOperator(BaseOperator):
    """
    Custom operator for extracting weather data from multiple cities
    """
    
    template_fields = ['cities', 'api_key']
    ui_color = '#DDA0DD'
    
    @apply_defaults
    def __init__(
        self,
        cities: List[str],
        api_key: Optional[str] = None,
        parallel_requests: bool = False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.cities = cities
        self.api_key = api_key
        self.parallel_requests = parallel_requests
    
    def execute(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute weather data extraction for multiple cities"""
        from scripts.extract_weather import extract_multiple_cities
        
        self.log.info(f"Extracting weather data for {len(self.cities)} cities")
        
        try:
            weather_data_list = extract_multiple_cities(
                cities=self.cities,
                **context
            )
            
            self.log.info(f"Successfully extracted weather data for {len(weather_data_list)} cities")
            return weather_data_list
            
        except Exception as e:
            self.log.error(f"Failed to extract multi-city weather data: {str(e)}")
            raise AirflowException(f"Multi-city weather extraction failed: {str(e)}")


class WeatherQualityCheckOperator(BaseOperator):
    """
    Custom operator for weather data quality checks
    """
    
    template_fields = ['input_key']
    ui_color = '#FFB6C1'
    
    @apply_defaults
    def __init__(
        self,
        input_key: str = 'transformed_weather_data',
        temperature_range: tuple = (-50, 60),
        humidity_range: tuple = (0, 100),
        pressure_range: tuple = (870, 1085),
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.input_key = input_key
        self.temperature_range = temperature_range
        self.humidity_range = humidity_range
        self.pressure_range = pressure_range
    
    def execute(self, context: Dict[str, Any]) -> bool:
        """Execute data quality checks"""
        self.log.info("Starting weather data quality checks")
        
        try:
            # Get transformed data
            data = context['task_instance'].xcom_pull(key=self.input_key)
            
            if not data:
                raise AirflowException(f"No data found with key: {self.input_key}")
            
            issues = []
            
            # Check temperature
            temp = data.get('temperature', 0)
            if not (self.temperature_range[0] <= temp <= self.temperature_range[1]):
                issues.append(f"Temperature {temp}°C outside normal range {self.temperature_range}")
            
            # Check humidity
            humidity = data.get('humidity', 0)
            if not (self.humidity_range[0] <= humidity <= self.humidity_range[1]):
                issues.append(f"Humidity {humidity}% outside normal range {self.humidity_range}")
            
            # Check pressure
            pressure = data.get('pressure', 0)
            if pressure > 0 and not (self.pressure_range[0] <= pressure <= self.pressure_range[1]):
                issues.append(f"Pressure {pressure} hPa outside normal range {self.pressure_range}")
            
            # Check required fields
            required_fields = ['city', 'temperature', 'humidity', 'timestamp']
            for field in required_fields:
                if field not in data or data[field] is None:
                    issues.append(f"Missing required field: {field}")
            
            if issues:
                self.log.warning(f"Data quality issues found: {'; '.join(issues)}")
                # You can choose to fail or just warn
                # raise AirflowException(f"Data quality check failed: {'; '.join(issues)}")
            else:
                self.log.info("All data quality checks passed")
            
            return len(issues) == 0
            
        except Exception as e:
            self.log.error(f"Data quality check failed: {str(e)}")
            raise AirflowException(f"Data quality check error: {str(e)}")


class WeatherHistoryOperator(BaseOperator):
    """
    Custom operator for retrieving weather history
    """
    
    template_fields = ['city', 'days_back']
    ui_color = '#FFA07A'
    
    @apply_defaults
    def __init__(
        self,
        city: Optional[str] = None,
        days_back: int = 7,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.city = city
        self.days_back = days_back
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute weather history retrieval"""
        from scripts.load_weather import get_weather_history
        
        self.log.info(f"Retrieving weather history for last {self.days_back} days")
        
        try:
            history_df = get_weather_history(
                city=self.city,
                days=self.days_back
            )
            
            history_stats = {
                'record_count': len(history_df),
                'date_range': {
                    'start': str(history_df['timestamp'].min()) if len(history_df) > 0 else None,
                    'end': str(history_df['timestamp'].max()) if len(history_df) > 0 else None
                },
                'cities': history_df['city'].unique().tolist() if len(history_df) > 0 else []
            }
            
            if len(history_df) > 0:
                # Add temperature statistics
                history_stats['temperature_stats'] = {
                    'avg': round(history_df['temperature'].mean(), 2),
                    'min': round(history_df['temperature'].min(), 2),
                    'max': round(history_df['temperature'].max(), 2)
                }
            
            self.log.info(f"Retrieved {history_stats['record_count']} historical records")
            
            # Store in XCom for downstream tasks
            context['task_instance'].xcom_push(key='weather_history_stats', value=history_stats)
            
            return history_stats
            
        except Exception as e:
            self.log.error(f"Failed to retrieve weather history: {str(e)}")
            raise AirflowException(f"Weather history retrieval failed: {str(e)}")


# Sensor for monitoring weather conditions
class WeatherAlertSensor(BaseOperator):
    """
    Custom sensor for weather condition alerts
    """
    
    template_fields = ['city', 'temperature_threshold', 'humidity_threshold']
    ui_color = '#FF6347'
    
    @apply_defaults
    def __init__(
        self,
        city: str = 'London',
        temperature_threshold: Optional[float] = None,
        humidity_threshold: Optional[float] = None,
        wind_speed_threshold: Optional[float] = None,
        alert_type: str = 'email',  # email, log, slack
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.city = city
        self.temperature_threshold = temperature_threshold
        self.humidity_threshold = humidity_threshold
        self.wind_speed_threshold = wind_speed_threshold
        self.alert_type = alert_type
    
    def execute(self, context: Dict[str, Any]) -> bool:
        """Check weather conditions and send alerts if thresholds are exceeded"""
        from scripts.extract_weather import extract_weather_data
        
        self.log.info(f"Checking weather conditions for {self.city}")
        
        try:
            # Get current weather data
            weather_data = extract_weather_data(city=self.city, **context)
            
            if not weather_data:
                raise AirflowException("No weather data available for alert check")
            
            alerts = []
            
            # Check temperature threshold
            if self.temperature_threshold:
                temp = weather_data['current']['temp_c']
                if temp > self.temperature_threshold:
                    alerts.append(f"High temperature alert: {temp}°C > {self.temperature_threshold}°C")
            
            # Check humidity threshold
            if self.humidity_threshold:
                humidity = weather_data['current']['humidity']
                if humidity > self.humidity_threshold:
                    alerts.append(f"High humidity alert: {humidity}% > {self.humidity_threshold}%")
            
            # Check wind speed threshold
            if self.wind_speed_threshold:
                wind_speed_kph = weather_data.get('current', {}).get('wind_kph', 0)
                wind_speed_ms = wind_speed_kph * 0.277778  # Convert kph to m/s
                if wind_speed_ms > self.wind_speed_threshold:
                    alerts.append(f"High wind speed alert: {wind_speed_ms:.1f} m/s > {self.wind_speed_threshold} m/s")
            
            if alerts:
                alert_message = f"Weather alerts for {self.city}: " + "; ".join(alerts)
                self.log.warning(alert_message)
                
                # Send alert based on type
                if self.alert_type == 'email':
                    # You can implement email sending here
                    self.log.info("Email alert would be sent")
                elif self.alert_type == 'slack':
                    # You can implement Slack notification here
                    self.log.info("Slack alert would be sent")
                
                return True  # Alert triggered
            else:
                self.log.info("No weather alerts triggered")
                return False  # No alerts
                
        except Exception as e:
            self.log.error(f"Weather alert check failed: {str(e)}")
            raise AirflowException(f"Weather alert sensor failed: {str(e)}")
