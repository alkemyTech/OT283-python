import os, logging, time
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import exc, create_engine, inspect
from decouple import config

# The log parameters are established
logger = logging.getLogger('Universidades B')

logging.basicConfig(
                    filename='connection_extraction.log',
                    level=logging.INFO,
                    datefmt='%Y-%M-%D',
                    format='%(asctime)s- %(logger)s - %(mensaje)s'
                    )

# The database connection parameters are given
db_name = config('DB_NAME')
db_user = config('DB_USER')
db_pass = config('DB_PASS')
db_host = config('DB_HOST')
db_port = config('DB_PORT')

# The DAG properties are defined
default_args = {
                'owner': 'Airflow',
                'retries': 5,
                'retry_delay': timedelta(minutes = 1)
                }

# Database connection function is created

def connect_db():
    """
    Function created to establish connection to database.
    It is executed as long as there are 5 or less retries and the tables exist.
    """
    retry_count = 0
    retry_flag = True

    while retry_flag and retry_count < 5:
        try:
            engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user,db_pass,db_host,db_port,db_name))
            engine.connect()
            logger.info('Connected to database succesfully')
            insp = inspect(engine)
            if insp.has_table('flores_comahue') and insp.has_table('salvador_villa_maria'):
                retry_flag = False
                logger.info('The database has the tables needed. Proceeding.')
            else:
                retry_count += 1
                time.sleep(60)
                logger.info('Unable to connect, will wait 60 seconds and try again.')
        except exc.SQLAlchemyError:
            retry_count += 1
            time.sleep(60)
            logger.info('Connection error, will wait 60 seconds and try again.')
    return engine

# ETL functions are created
def extract_db(**kwargs):
    """
    Function that extracts the queries from the database and creates the csv files.
    """
    query_list = [v for k, v in kwargs.items() if type(v) == str and 'sql_filename' in k]
    if not query_list:
        logger.info('At the moment there are no queries to process')
    
    engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user,db_pass,db_host,db_port,db_name))
    # Extracting all data from query names injected in the dag op_args parameter
    for query_file in query_list:
        with engine.connect() as con:
            
            with open(f"dags/sql/{query_file}") as file:
                query = file.read()              
                result_q = con.execute(query)
                logger.info('Adding query results to a dataframe')
                df = pd.DataFrame(result_q)
                os.makedirs('files',exist_ok=True)
                df.to_csv('files/nacional_salvador.csv', index=False)
                logger.info('Succesfully created the csv files')
                return

def transform_with_pandas():
    logger.info('Transforming data')
    pass

def load_to_s3():
    logger.info('Loading data to s3')
    pass


# DAG is defined as well as tasks to execute
with DAG(
        'OT283-38-46-54',
        description = 'DAG para conectar y extraer Universidades B',
        default_args = default_args,
        schedule_interval = timedelta(hours = 1),
        start_date=datetime(2022, 8, 30)
        ) as dag:
    
    logger.info('Beginning tasks')

    connect_db_task = PythonOperator(
                                    task_id = 'connect_db_task',
			                        python_callable = connect_db
                                    )
    
    extract_task = PythonOperator(
                                  task_id = 'extract',
                                  python_callable = extract_db,
                                  op_kwargs = {'sql_filename' : 'OT283-14.sql'}
                                 )
    
    transform_task = PythonOperator(
                                    task_id = 'transform_universidadesB',
			                        python_callable = transform_with_pandas
                                    )
    
    load_task = PythonOperator(
                              task_id = 'load_universidadesB',
		                      python_callable = load_to_s3
                              )

    connect_db_task >> extract_task >> transform_task >> load_task