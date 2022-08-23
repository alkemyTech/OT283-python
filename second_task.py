import logging
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator


# setting up the login
logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%M-%D'
)

## Creating the tasks template
def extract():
    # Extract data from sql
    logging.info('extracting data')


def transform():
    #transform the extracted data
    logging.info('transforming data')

def load():
    #load data 
    logging.info('loading data')


# Definimos propiedades del dag
with DAG(
    'OT283-23',
    description='Creating a template DAG for posterior use',
    schedule_interval=timedelta(hours=1), #repeat each hour
    start_date=datetime(2022,9,23),

) as dag:
    ## Creating the tasks for the DAG
    extract_task = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract,
    )
    transform_task = PythonOperator(
        task_id = 'transform_data',
        python_callable= transform,
    )
    load_task = PythonOperator(
        task_id = 'load_data',
        python_callable= load,
    )

    logging.info('executing the tasks')
    extract_task >> transform_task >> load_task

