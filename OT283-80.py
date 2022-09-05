import airflow
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

def upload_to_s3(*args):
    """
    This Function takes the connection that was set within airflow interface and upload to S3 AWS
    
    ----------------

    Parameters:
    File name : str
    Key : str
    bucket_name : str
    """
    logging.info('initialize upload to s3 function')
    arguments = [value for value in args]
    filename = arguments[0]
    key = arguments[1]
    bucket_name = arguments[2]

    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name,acl_policy='public-read') 
    logging.info('end of function upload to s3')

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








#Inintialize DAG 
with DAG(
    'OT283-79',
    description='Define a dummy DAG for Alkemy Sprint1',
    #As required the DAG should execute every hour everyday
    schedule_interval=timedelta(hours=1), 
    start_date=datetime(2022,9,4),
    ) as dag:
    #Using PythonOperator task are set with python callable functions
    extract_task = PythonOperator(task_id = 'extract', python_callable = extract)
    transform_task = PythonOperator(task_id = 'transform', python_callable= transform)
    load_task = PythonOperator(task_id = 'uploadS3', python_callable=upload_to_s3 , op_args=['r_query2_rio_cuarto_interamericana.txt','r_query2_rio_cuarto_interamericana.txt','cohorte-agosto-38d749a7'])

    logging.info('Executing scheduled tasks...')
    extract_task >> transform_task >> load_task