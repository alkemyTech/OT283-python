import os
from decouple import config
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from uploads3 import Uploads3

schedule = timedelta(hours=1)
defaultargs = {
    'owner':'Airflow',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dags = []
BUCKET = config('BUCKET_NAME')
files=[]

# check all txt files that are within working directory and append file name to lists
current=os.getcwd()
for file in os.listdir("%s"%current):
    if file.endswith(".txt"):
        files.append(os.path.join("%s"%current, file))
        dags.append("upload_to_S3_%s"%file)

    




def create_dag(dag_id, schedule, default_args, *args):

    arguments = [arg for arg in args]

    
    with DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        start_date=datetime(2022,9,5),
    ) as dag:

        for n in range(len(arguments)):
            upS3=Uploads3(arguments[n],argments[n],BUCKET)
            task = PythonOperator(                            
                            task_id= f'{dag_id}',
                            python_callable =upS3.upload_to_s3,
                            dag=dag
                )
    return dag


for n in range(len(files)):
    globals()[dags[n]] = create_dag(dags[n],schedule,defaultargs,files[n])