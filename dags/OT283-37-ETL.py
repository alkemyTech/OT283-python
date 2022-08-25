from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
import logging

def extract_from_db():
    pass

def transform_data_uni():
    pass

def load_to_s3():
    pass

# Configuracion del loggs
logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%M-%D',
    format='%(asctime)s- %(logger)s - %(mensaje)s'
)

default_args = {
    'owner':'Airflow',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    'dag_OT283-21',
    description='Configuracion DAG para Univeridades del grupo A',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,8,23),
) as dag:

    logging.info("Comenzando tareas")

    # Tareas que se dejan armadas para luego ser utilizadas mas adelante en el ETL
    extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract_from_db,
        retries=5,
        retry_delay=timedelta(minutes=5),
        dag=dag
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data_uni,
        dag=dag
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_to_s3,
        dag=dag
    )

    # Flujo de ejecucion
    extract >> transform >> load
