# Importamos las dependencias necesarias
import logging
from pathlib import Path
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Configuramos los logs
now = datetime.now()
year = now.year
month = now.month
day = now.day
date = f"{day:02d}-{month:02d}-{year}"
today_date = date.format(
                        year = year, 
                        month = month, 
                        day = day
                        )
ROOT_DIR = Path("../")
logging.basicConfig(
                    filename = f"{ROOT_DIR}../logs/etl_run-{today_date}.log", 
                    level = "DEBUG"
                    )


# Definimos las propiedades del DAG
default_args = {
                "owner": "airflow",
                "depends_on_past": False,
                "email": ["airflow@example.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 1,
                "retry_delay": timedelta(minutes = 5),
                }

# Creamos las funciones que intervendrán en el proceso
def get_data():
    logging.info("getting the data")

def transform_data():
    logging.info("transforming the data")

def load_data():
    logging.info("loading the data")

# Definimos el DAG
with DAG(
        "elt_dag",
        default_args = default_args,
        description =  "DAG para el procesamiento y carga de datos a S3",
        schedule_interval = timedelta(hours = 1),
        start_date = datetime(2022, 8, 23),
        tags = ["alkemy_acceleration_sptr01"],
        ) as dag:
        get_data_task = PythonOperator(
                                       task_id = "get_data", 
                                       python_callable = get_data, 
                                       retries = 5, 
                                       retry_delay = timedelta(minutes=5)
                                          )
        transform_data_task = PythonOperator(
                                            task_id = "transform_data", 
                                            python_callable = transform_data
                                            )
        load_data_task = PythonOperator(
                                        task_id = "load_data", 
                                        python_callable = load_data
                                       )

        get_data_task >> transform_data_task >> load_data_task