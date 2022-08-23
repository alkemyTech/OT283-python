from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
def extract(): #extract data from sql
        logging.info('Extracting')

def transform(): #transform data with pandas
    logging.info('Transforming')

def load(): #upload data to S3
    logging.info('Loading')


#Start dag
with DAG(
    'OT283-24',
    default_args=default_args,
    description='Task2',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 8, 22),
    catchup=False
    ) as dag:

        logging.info('Processing tasks')

        extract_task = PythonOperator(task_id='extract', python_callable= extract)
        transform_task = PythonOperator(task_id='transform', python_callable= transform)
        load_task = PythonOperator(task_id='load', python_callable= load)
    
    
        extract_task >> transform_task >> load_task