import logging
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
from decouple import config
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from sqlalchemy import exc, create_engine, inspect

# root path
ROOT_DIR = Path('../')
today = datetime.now()
# database credetials in .env file
DB_USER = config('DB_USER')
PASSWORD = config('PASSWORD')
HOST = config('HOST')
PORT = config('PORT')
DB_NAME = config('DB_NAME')

def verify_db_connection():
    """
    Function that connect with database
        * We define a function that control the execution, when execution ocure more than 5 times the database connection end
    """
    flag = False
    count = 0
    logging.info(" connecting to DB ")
    while not flag and count < 5:
        logging.info(f"Intento N°{count}")
        try:
            # We use env variable to connecto to postgre database
            engine = create_engine(f"postgresql://{DB_USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")
            conn = engine.connect()
            logging.info(" Connection succed ")

            # Check that tables exist, if not we connect one more time to the database
            if engine.dialect.has_table(conn,"flores_comahue") and engine.dialect.has_table(conn, 'salvador_villa_maria'):  
                logging.info(" Tables exist ")
                flag=True
            else:
                # The count variable incremet a unit, and sleep 30 seconds between executions to avoid connections delays
                logging.info(" Tables not exist ")
                count = count + 1
                time.sleep(30)
            
        except exc.SQLAlchemyError:
            # If occurs an arror, the count variable increment a unit, and the code sleep 30 seconds until the next execution
            count = count + 1
            time.sleep(30)

    return True

logger = logging.getLogger('Universidades A')

# logs configuration
logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%M-%D',
    format='%(asctime)s- %(logger)s - %(mensaje)s'
)

default_args = {
    'owner':'Airflow',
    'retries':5,
    'retry_delay':timedelta(seconds=30)
}

# We define DAG configuration
with DAG(
    'check_db_connection',
    description=' Check your DB connection ',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022,8,23),
) as dag:

    task_check_db_connection = PythonOperator(
        task_id = 'check_db_connection',
        python_callable = verify_db_connection,
        dag=dag
    )
