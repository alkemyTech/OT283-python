import time, os, logging
from datetime import timedelta, datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from decouple import config, AutoConfig
from sqlalchemy import exc, create_engine, inspect

from utils.check_db_connection import check_db_connection
from utils.etl import extract, transform, load

# Setting up the logger
logger = logging.getLogger('Universidades C')

logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%M-%D'
)

# loading the env variables 
db_host = config('DB_HOST')
db_port = config('DB_PORT')
db_user = config('DB_USER')
db_pass = config('DB_PASSWORD')
db_name = config('DB_NAME')

# Creating the db engine
engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user,db_pass,db_host,db_port,db_name))


# default arguments for the connection check
default_args = {
    'owner':'Airflow',
    'retries':5,
    'retry_delay':timedelta(seconds=30)
}


# Definimos propiedades del dag
with DAG(
    'Main_dag',
    description='Main dag for the etl proyect',
    schedule_interval=timedelta(hours=1), #repeat each hour
    start_date=datetime(2022,8,23),

) as dag:
    ## Check the database connection
    task_check_db_connection = PythonOperator(
        task_id = 'check_db_connection',
        python_callable = check_db_connection,
        op_kwargs={'engine':engine, 'max_retries':default_args['retries']}  ## engine data for db connection
    )
    ## Extract the data from the database
    extract_task = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract,
        op_kwargs={
            'sql_filename':'query_jujuy.sql',
            'sql_filename2': 'query_palermo.sql',
            'engine':engine  ### engine data for db connection
            }
    )
    transform_task = PythonOperator(
        task_id = 'transform_data',
        python_callable= transform,
    )
    load_task = PythonOperator(
        task_id = 'load_data',
        python_callable= load,
    )

    task_check_db_connection >> extract_task >> transform_task >> load_task

