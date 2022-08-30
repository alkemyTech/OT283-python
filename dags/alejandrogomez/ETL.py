from airflow import DAG
from datetime import timedelta, datetime
from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
import logging
from alejandrogomez.etl_functions import extract_db
from alejandrogomez.etl_functions import process_data_uni
from alejandrogomez.etl_functions import upload_to_s3



logger = logging.getLogger('Universidades A')

# logs configuration
logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%M-%D',
    format='%(asctime)s- %(logger)s - %(mensaje)s'
)

# airflow default arguments configuration
default_args = {
    'owner':'Airflow',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    'dag_OT283-ETL',
    description='Configuracion DAG para Univeridades del grupo A',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,8,23),
) as dag:

    logging.info("Comenzando tareas")

    # task that will be use by dags
    extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract_db,
        retries=5,
        retry_delay=timedelta(minutes=5),
        op_args=['flores.sql','villaMaria.sql'],
        dag=dag
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=process_data_uni,
        dag=dag
    )

    load = PythonOperator(
        task_id='load',
        python_callable=upload_to_s3,
        dag=dag
    )

    # execution flow
    extract >> transform >> load

if __name__ == '__main__':
    extract_db('flores.sql','villaMaria.sql')