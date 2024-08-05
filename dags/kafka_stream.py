import uuid
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


transaction_dags = DAG(
    dag_id='dag_1',
    start_date = datetime.datetime(2024, 8, 5),
    schedule = '@daily',
    default_args = {
        'owner' : 'de_hmdn'
    }
)

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
    data['dob'] = res['dob']['date']
    data['gender'] = result['gender']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['large']
    data['city'] = location['city']
    data['state'] = location['state']
    data['postcode'] = location['postcode']
    data['country'] = location['country']
    data['street'] = f"{str(location['street']['number'])} {location['street']['name']}"
    return data


