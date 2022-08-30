from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from sqlalchemy import exc, create_engine, inspect
from datetime import datetime, timedelta
import logging
import psycopg2
import pandas as pd
import os
from decouple import config

default_args = {
     'owner': 'airflow',
     'retries': 1,
     'retry_delay': timedelta(minutes=5)
}

#create logger
logger = logging.getLogger('Universidades D')
logger.setLevel(logging.DEBUG)
#create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
#create formater
format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')


#Config decouple module
user = config('USER')
password = config('PASSWORD')
host = config('HOST')
port = config('PORT')
db_name = config('DB_NAME')


#DB_connection
def check_db_connection():

    #flag variable
    retry_flag = True
    #counter variable
    retry_count = 0
    while retry_flag and retry_count < 5:
        try:
            #connect the database            
            engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")  #postgresql://user:password@host:port/db" 
            engine.connect()
            ins = inspect(engine)
            #check if tables exists
            if ins.has_table('jujuy_utn') and ins.has_table('palermo_tres_de_febrero'):
                retry_flag = False
            else:
                retry_count = retry_count + 1                
                time.sleep(60)
            print(engine.table_names())
        except exc.SQLAlchemyError:
            logging.info("Connection error")
            retry_count=retry_count + 1
            time.sleep(60)

def extract(): #extract data from sql
    logging.info('Extracting')


    #connect with database
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")  #postgresql://user:password@host:port/db" 
    engine.connect()

    #read the sql files with the queries
    SQL_DIR = os.path.abspath(os.path.dirname(__file__))

    sql1_ub = os.path.join(SQL_DIR, "universidad_nacional_tres_de_febrero.sql")
    with open(sql1_ub) as  universidad1:
        unf= universidad1.read()         
    #apply the sql query to extracted data from postgresql
    unf_filtered = pd.read_sql(unf,engine)
    print(unf_filtered)
    #create a directory
    os.makedirs('my_files', exist_ok=True)
    #create a cvs file as a result of the query on the extracted data
    unf_filtered.to_csv('my_files/universidad_nacional_tres_de_febrero.csv')
       

    #repeat the process for the second query
    sql2_ub = os.path.join(SQL_DIR, "universidad_tecnologica_nacional.sql")
    with open(sql2_ub, encoding='UTF-8') as universidad2:
        utn= universidad2.read()
    utn_filtered = pd.read_sql(utn,engine) 
    print(utn_filtered)
    utn_filtered.to_csv('my_files/universidad_tecnologica_nacional.csv')
 
    

    #add formater to console handler
    console_handler.setFormatter(format)
    #add console handler to logger
    logger.addHandler(console_handler)
    logger.debug('Información extraída')


def transform(): #transform data with pandas
    logging.info('Transforming')

def load(): #upload data to S3
    logging.info('Loading')

       
#start dag
with DAG(
      'OT283-48',
       default_args=default_args,
       description='Task2 - Sprint2',
       schedule_interval=timedelta(hours=1),
       start_date=datetime(2022, 8, 29),
       catchup=False
       ) as dag:

           logging.info('Processing tasks')
           connection_task = PythonOperator(task_id='db_connection', python_callable= check_db_connection,dag=dag)
           extract_task = PythonOperator(task_id='extract', python_callable= extract, dag=dag)
           transform_task = PythonOperator(task_id='transform', python_callable= transform, dag=dag)
           load_task = PythonOperator(task_id='load', python_callable= load, dag=dag)
   
    
           connection_task >> extract_task >> transform_task >> load_task    

    