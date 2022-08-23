import logging
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import time
from sqlalchemy import exc, create_engine, inspect
from decouple import config


# Configuracion del login
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


def check_db_connection():
    logging.info('checking data base connection with {} retries'.format(default_args['retries']))

    retry_flag = True
    retry_count = 0
    while retry_flag and retry_count < default_args['retries']:
        try:
            logging.info(f'connecting to', "postgresql://{}:{}@{}:{}/{}".format(db_user,db_pass,db_host,db_port,db_name))
            engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user,db_pass,db_host,db_port,db_name))
            engine.connect()
            logging.info('success at connecting to the database')
            insp = inspect(engine)
            # se comprueba si existen las tablas, de no ser asi se reiintenta la conexion
            if insp.has_table("jujuy_utn") and insp.has_table("palermo_tres_de_febrero"):
                logging.info('found jujuy_utn and palermo databases')
                retry_flag = False
            else:
                logging.info('couldnt find the tables in the database. Retrying')
                logging.info('retrying')
                logging.info('{} retires left'.format((default_args['retries'] - retry_count)))
                retry_count += 1
                time.sleep(60)
        except exc.SQLAlchemyError:
            logging.info("There was a connection error")
            logging.info('{} retires left'.format((default_args['retries'] - retry_count)))
            #se incrementa la variable de control en caso de que se produzca un error
            retry_count += 1
            #se espera 1 minuto antes de el siguiente reintento
            time.sleep(60)



# Definimos propiedades del dag
with DAG(
    'check_db_connection',
    description='Comprobar la conexion a la base de datos',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022,8,23),
) as dag:
    task_check_db_connection = PythonOperator(
        task_id = 'check_db_connection',
        python_callable = check_db_connection,
        dag=dag
    )