import uuid
import requests
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
import logging

dag = DAG(
    dag_id='dag_1',
    start_date = datetime.datetime(2024, 8, 5),
    schedule = '@daily',
    default_args = {
        'owner' : 'de_hmdn'
    }
)

#Task
def get_data():
    result = requests.get('https://randomuser.me/api')
    result = result.json()
    result = result['results'][0]
    return result

def data_formatting(result):
    data = {}
    location = result['location']
    data['first_name'] = result['name']['first']
    data['last_name'] = result['name']['last']
    data['dob'] = result['dob']['date']
    data['gender'] = result['gender']
    data['email'] = result['email']
    data['username'] = result['login']['username']
    data['registered_date'] = result['registered']['date']
    data['phone'] = result['phone']
    data['picture'] = result['picture']['large']
    data['city'] = location['city']
    data['state'] = location['state']
    data['postcode'] = location['postcode']
    data['country'] = location['country']
    data['street'] = f"{str(location['street']['number'])} {location['street']['name']}"
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

task_1 = PythonOperator(
 task_id='task_1',
 python_callable=stream_data,
 dag=dag,
)

# task dependencies
task_1
