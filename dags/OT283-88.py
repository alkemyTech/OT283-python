from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os
from decouple import config

#create logger
logger = logging.getLogger('Universidades D')
logger.setLevel(logging.DEBUG)
#create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
#create formater
format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')


#Config decouple module
user = config('USER')
password = config('PASSWORD')
host = config('HOST')
port = config('PORT')
db_name = config('DB_NAME')
path_files = config('PATH')

def create_dag(dag_id, schedule, default_args):

    #DB connection
    def extract():
        logging.info('Extracting')

    def transform(): #transform data with pandas
        logging.info('Transforming')

    def load(): #upload data to S3
        logging.info('Loading')

    dag = DAG(dag_id=dag_id,
            schedule_interval=schedule,
            default_args=default_args
            )
       
    #start dag
    with dag:
        logging.info('Processing tasks')
        extract_task = PythonOperator(task_id='extract', python_callable= extract, dag=dag)
        transform_task = PythonOperator(task_id='transform', python_callable= transform, dag=dag)
        load_task = PythonOperator(task_id='load', python_callable= load, dag=dag)
        extract_task >> transform_task >> load_task

    return dag               
        
            
#list the files

txt_files = os.listdir(f"{path_files}")
#assign a unique name for each file
for n, name in enumerate(txt_files,start=1):
    dag_id= f'{n}_{name}'
    default_args = {
            'owner': 'airflow',
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
    schedule = '@daily'
    dag_number = n

    globals()[dag_id]= create_dag(dag_id,
                                dag_number,   
                                default_args,                                
                                schedule,                                
                                catchup=False)


