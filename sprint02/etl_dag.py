# Importamos las dependencias necesarias
from connect_dag import start_connct
from confi import db_user
from constans import LOG_DIR, SCHEMA_NAME
from elt_funtions import get_table, transform_dts

import logging
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

#   Definimos el formato de fecha 
now = datetime.now()
year = now.year
month = now.month
day = now.day
date = f"{year}-{month:02d}-{day:02d}"                                      #   Establecemos los place holders
today_date = date.format(                                               
                        year = year, 
                        month = month, 
                        day = day
                        )

#   Establecemos las configuración del log
logging.basicConfig(
                    filename = f"{LOG_DIR}/{today_date}-{db_user}.log",     #   Se generará un archivo .log el cual 
                                                                            #   tendrá en el nombre el siguiente formato
                                                                            #   %Y-%m-%d - nombre_logger(usuario de 
                                                                            #   la base de datos)
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

def get_data():
    logging.info(f"getting the data from {table_name}")

def transform_data():
    logging.info(f"transforming the data from {table_name}")

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
                                        retry_delay = timedelta(minutes=5),
                                        dag = dag
                                      )
        transform_data_task = PythonOperator(
                                            task_id = "transform_data", 
                                            python_callable = transform_data,
                                            dag = dag
                                            )
        load_data_task = PythonOperator(
                                        task_id = "load_data", 
                                        python_callable = load_data,
                                        dag = dag
                                       )

        get_data_task >> transform_data_task >> load_data_task