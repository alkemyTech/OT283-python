from datetime import datetime, timedelta
import logging
from decouple import config
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

path_file=config('PATH')

def upload_txt_unf(filename,key,bucket_name):
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename,key=key,bucket_name=bucket_name)

with DAG(
      'OT283-76',
       description='upload unf txt',
       schedule_interval=timedelta(hours=1),
       start_date=datetime(2022, 9, 4),
       catchup=False
) as dag: 

        logging.info('Processing tasks')
        upload_unf = PythonOperator(task_id='upload_txt_unf',
        python_callable= upload_txt_unf,
        op_kwargs={ 
            'filename': f'{path_file}/universidad_nacional_tres_de_febrero.txt',
            'key': 'universidad_nacional_tres_de_febrero.txt',
            'bucket_name': 'cohorte-agosto-38d749a7'
        }
    )
            
