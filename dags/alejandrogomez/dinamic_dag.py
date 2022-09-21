import os
from ast import arg
from decouple import config
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from alejandrogomez.etl_functions import upload_to_s3
from alejandrogomez.etl_functions import DATA_DIR


schedule = timedelta(hours=1)
defaultargs = {
    'owner':'Airflow',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}
names = []
dags = []
BUCKET = config('BUCKET_NAME')
# Extract the list of files in the specified directory
files = os.listdir(f'{DATA_DIR}/files/alejandrogomez')

for file in files:
    # Inside each file we see one that matches with .exe extension and then we split the file to save the name and dags variables with that name.
    if '.csv' in file:      
        name_exten = file.split('.')
        names += [name_exten[0]]
        dags += [f'task_upload_to_s3_{name_exten[0]}']


def create_dag(dag_id, schedule, default_args, *args):
    
    """
        Function that create dag dinamically
            Input:
                * Dag_id -> list of dags_ids
                * Schedule
                * Default_args
                * Names -> txt files name
            Output:
                * Dag
    """

    arguments = [arg for arg in args]

    with DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        start_date=datetime(2022,8,23),
    ) as dag:

        for n in range(len(arguments)):
             task = PythonOperator(                            
                            task_id= f'{dag_id}',
                            python_callable =upload_to_s3,
                            # Parameters that will be used in the upload function, the first is file directory, second is the file name and finally the bucket name
                            op_kwargs={
                                    'filename':f'{DATA_DIR}/files/alejandrogomez/{arguments[n]}.txt',
                                    'key':f'{arguments[n]}.txt',
                                    'bucket_name':f'{BUCKET}'
                                },
                            dag=dag
                )
    return dag


for n in range(len(names)):
    # We go through the length of name variable which is the number of files we have and in each one we execute the function that creates the dag dynamically
    globals()[dags[n]] = create_dag(dags[n],schedule,defaultargs,names[n])
