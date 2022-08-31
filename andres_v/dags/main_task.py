import time, os, logging
from datetime import timedelta, datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from decouple import config, AutoConfig
from sqlalchemy import exc, create_engine, inspect


# Configuracion del login
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

# default arguments for the connection check
default_args = {
    'owner':'Airflow',
    'retries':5,
    'retry_delay':timedelta(seconds=30)
}

logging.info('Env variables initialized')

def check_db_connection(max_retries=1):
    #logging.info('checking data base connection with {} retries'.format(default_args['retries']))

    retry_flag = True
    retry_count = 0
    while retry_flag and retry_count < max_retries:
        try:
            #logging.info("connecting to postgresql://{}:{}@{}:{}/{}".format(db_user,db_pass,db_host,db_port,db_name))
            engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user,db_pass,db_host,db_port,db_name))
            conn = engine.connect()
            #logging.info('success at connecting to the database')
            insp = inspect(engine)

            # se comprueba si existen las tablas, de no ser asi se reiintenta la conexion
            if engine.dialect.has_table(conn,"jujuy_utn") and engine.dialect.has_table(conn,"palermo_tres_de_febrero"):
                #logging.info('found jujuy_utn and palermo databases')
                retry_flag = False
            else:
                #logging.info('couldnt find the tables in the database. Retrying')
                #logging.info('retrying')
                #logging.info('{} retires left'.format((max_retries - retry_count)))
                retry_count += 1
                time.sleep(60)
        except exc.SQLAlchemyError:
            #logging.info("There was a connection error")
            #logging.info('{} retires left'.format((max_retries - retry_count)))
            #se incrementa la variable de control en caso de que se produzca un error
            retry_count += 1
            #se espera 1 minuto antes de el siguiente reintento
            time.sleep(60)

def extract(**kwargs):
    ### getting the name of the sql files
    lista_queries = [v for k, v in kwargs.items() if type(v) == str and 'sql_filename' in k]
    if not lista_queries:
        logging.info("Theres no queries to process. skipping task")
        return 0
    #logging.info(('list with the names of sql files{}'.format(lista_queries)))
    
    ### use the return of the connection to get here or something
    engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user,db_pass,db_host,db_port,db_name))
    ### For Loop to extract data from all the query names injected in the dag op_args parameter
    for query_file in lista_queries:
        with engine.connect() as con:
            
            with open(f"dags/sql/{query_file}") as file:
                query = file.read()              
                result_q = con.execute(query)
                #logging.info('Adding query results to a dataframe')
                df = pd.DataFrame(result_q)
                os.makedirs('files',exist_ok=True)
                df.to_csv('files/query_jujuy.csv', index=False)
                #logging.info('succesfully created the csv files')
                return 0


def transform():
    #transform the extracted data
    logging.info('transforming data')
    pass

def load():
    #load data 
    logging.info('loading data')
    pass

# Definimos propiedades del dag
with DAG(
    'Main_dag',
    description='Main dag for the etl proyect',
    schedule_interval=timedelta(hours=1), #repeat each hour
    start_date=datetime(2022,8,23),

) as dag:
    task_check_db_connection = PythonOperator(
        task_id = 'check_db_connection',
        python_callable = check_db_connection,
        dag=dag
    )
    ## Creating the tasks for the DAG
    extract_task = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract,
        op_kwargs={'sql_filename':'query_jujuy.sql', 'sql_filename2': 'query_palermo.sql'}

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

