from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from sqlalchemy import exc, create_engine, inspect
from datetime import datetime, timedelta
import logging


def check_db_connection():
    #flag variable
    retry_flag = True
    #counter variable
    retry_count = 0

    while retry_flag and retry_count < 5:
        try:
            #connect the database            
            engine = create_engine("postgresql://{}:{}@{}:{}/{}")  #postgresql://user:password@host:port/db" 
            engine.connect()
            ins = inspect(engine)

            #check if tables exists
            if ins.has_table('jujuy_utn') and ins.has_table('palermo_tres_de_febrero'):
                retry_flag = False
            else:
                retry_count = retry_count + 1
                time.sleep(60)

        except exc.SQLAlchemyError:
            logging.info("Connection error")
            retry_count=retry_count + 1
            time.sleep(60)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
        
#start dag
with DAG(
    'OT283-32',
    default_args=default_args,
    description='Task3',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 8, 23),
    catchup=False
    ) as dag:

        logging.info('Processing tasks')
        connection_task = PythonOperator(task_id='db_connection', python_callable= check_db_connection,dag=dag)
        
    
    

    