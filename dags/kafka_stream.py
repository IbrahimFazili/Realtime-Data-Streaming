from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import json
import requests

default_args = {
    "owner": "Ibrahim Fazili", 
    "start_date": datetime(2024, 7, 29)
}

def get_data():
    response = requests.get('https://randomuser.me/api/').json()
    response = response['results'][0]
    return response

def format_data(response):
    data = {
        'first_name': response['name']['first'],
        'last_name': response['name']['last'],
        'gender': response['gender'],
        'address': (
            f"{str(response['location']['street']['number'])} {response['location']['street']['name']}, "
            f"{response['location']['city']}, {response['location']['state']}, {response['location']['country']}"
        ),
        'postcode': response['location']['postcode'],
        'email': response['email'],
        'username': response['login']['username'],
        'dob': response['dob']['date'],
        'age': response['dob']['age'],
        'registered_date': response['registered']['date'],
        'phone': response['phone'],
        'picture': response['picture']['medium']
    }
    return data

def stream_data():
    response = get_data()
    response = format_data(response)
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    producer.send('users_created', json.dumps(response).encode('utf-8'))

with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )