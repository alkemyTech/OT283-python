from datetime import datetime, timedelta
import logging
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from decouple import config

path_file=config('PATH')

def upload_txt_utn(filename,key,bucket_name):
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

with DAG(
      'OT283-75',
       description='upload utn txt',
       schedule_interval=timedelta(hours=1),
       start_date=datetime(2022, 9, 4),
       catchup = False
) as dag: 

        logging.info('Processing tasks')
        upload_utn = PythonOperator(task_id='upload_txt_utn',
        python_callable= upload_txt_utn,
        op_kwargs={ 
            'filename': f'{path_file}/universidad_tecnologica_nacional.txt',
            'key': 'universidad_tecnologica_nacional.txt',
            'bucket_name': 'cohorte-agosto-38d749a7'
        }
    )


            