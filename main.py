from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import json

default_args = {
    'owner': 'Alexander',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_weather_data():
    api_key = '463c3c950be55d1b38c6bec9ebec643e'
    city = 'London'
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
    response = requests.get(url)
    data = response.json()
    with open('/tmp/weather_data.json', 'w') as f:
        json.dump(data, f)

def process_weather_data():
    with open('/tmp/weather_data.json', 'r') as f:
        data = json.load(f)
    temp_kelvin = data['main']['temp']
    temp_celsius = temp_kelvin - 273.15
    processed_data = {
        'city': data['name'],
        'temperature_celsius': temp_celsius,
        'weather': data['weather'][0]['description']
    }
    df = pd.DataFrame([processed_data])
    df.to_csv('/tmp/processed_weather_data.csv', index=False)

def save_to_parquet():
    df = pd.read_csv('/tmp/processed_weather_data.csv')
    df.to_parquet('/tmp/weather.parquet')

dag = DAG(
    'weather_data_pipeline_dag',
    default_args=default_args,
    description='A simple weather data pipeline',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='process_weather_data',
    python_callable=process_weather_data,
    dag=dag,
)

t3 = PythonOperator(
    task_id='save',
    python_callable=save_to_parquet,
    dag=dag,
)

t1 >> t2 >> t3
