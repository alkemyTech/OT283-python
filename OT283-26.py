import airflow
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging
from airflow.operators.python_operator import PythonOperator


# setting up the login
logging.basicConfig(level=logging.INFO, datefmt='%Y-%M-%D')

default_args = {
            'owner': 'airflow',    
            #'start_date': airflow.utils.dates.days_ago(2),
            # 'end_date': datetime(),
            # 'depends_on_past': False,
            #'email': ['airflow@example.com'],
            #'email_on_failure': False,
            #'email_on_retry': False,
            # If a task fails, retry it once after waiting
            # at least 5 minutes
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }

def extract():
    """
    This function extract data from sql database
    """
    logging.info('Extracting...')


def transform():
    """
    This function transforms data that was previously extracted from 
    sql database
    """
    logging.info('Transforming extracted data...')

def load():
    """
    This function loads data to sql server
    """
    logging.info('Loading data...')


#Inintialize DAG 
with DAG(
    'OT283-26',
    description='Define a dummy DAG for Alkemy Sprint1',
    #As required the DAG should execute every hour everyday
    schedule_interval=timedelta(hours=1), 
    start_date=datetime(2022,8,24),
    ) as dag:
    #Using PythonOperator task are set with python callable functions
    extract_task = PythonOperator(task_id = 'extract', python_callable = extract)
    transform_task = PythonOperator(task_id = 'transform', python_callable= transform)
    load_task = PythonOperator(task_id = 'load', python_callable= load)

    logging.info('Executing scheduled tasks...')
    extract_task >> transform_task >> load_task
