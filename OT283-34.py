import airflow
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from decouple import config as cnfg
from dotenv import dotenv_values

# setting up the login
logging.basicConfig(level=logging.INFO, datefmt='%Y-%M-%D')


# check if dotenv file exist
config = dotenv_values(".env")
# get keyvalue names from dot env file
fileenv_keys=list(config.keys())
# get variable values from dot env file
config_dict=dict(config.items())


def get_engine(user,passwd, host, port, db):
    """
    this function checks if the database already exist
    and create the postgresql engine for the connection

    -----------

    user : user name
    passwd : password for database connection
    host : host name
    port : port id number
    db : name of the database

    """
    logging.info(f'initialized function to get engine for data base connection')
    url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
    if not database_exists(url):
        create_database(url)
        logging.info('database connection did not exist... creating new connection')
    engine = create_engine(url,pool_size=5, echo=False)
    logging.info(f'created connection to {}'.format(url))
    return engine


def get_engine_from_settings():
    """
    this function get the parameters to execute the function get_engine from dot env file
    and check if user used the expected variable names

    """

    keys=['USER_NAME','PASSWORD','HOST_NAME','PORT_ID','DATABASE_NAME']
    
    if not all(key in keys for key in fileenv_keys):
        loggin.info('Status for dot env file wrong check Exception')
        raise Exception('Bad config file, it must only exist the next env variables: \n %s\n %s \n %s\n %s \n %s'%(keys[0],keys[1],keys[2],keys[3],keys[4]))
    loggin.info('Status for dot env file good no exception were made')
    return get_engine(config_dict['USER_NAME'],
                   config_dict['PASSWORD'],
                   config_dict['HOST_NAME'],
                   config_dict['PORT_ID'],
                   config_dict['DATABASE_NAME'])


default_args = {
            'owner': 'airflow',    
            #'start_date': airflow.utils.dates.days_ago(2),
            # 'end_date': datetime(),
            # 'depends_on_past': False,
            #'email': ['airflow@example.com'],
            #'email_on_failure': False,
            #'email_on_retry': False,
            # If a task fails, retry it once after waiting
            # at least 5 minutes
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
        }



def verify_conn(table1:str, table2:str):
    loggin.info('Connection to database checking process with {} retries'.format(default_args['retries']))

    retry_flag = True
    retry_counts = 0
    while retry_flag and retry_counts < default_args['retries']:
        try:
            engine = get_engine_from_settings()
            engine.connect()
            logging.info('successful connection to database')
            insp = inspect(engine)
            # Check if tables exist at the database server, if not success retry the connection
            if insp.has_table(table1) and insp.has_table(table2):
                logging.info('tables %s and %s found on database'%(table1,table2))
                retry_flag = False
            else:
                logging.info('The tables %s and %s were not found in the databse...Retrying'%(table1,table2))
                logging.info('Retrying...')
                pending_retries=default_args['retries']-retry_counts
                logging.info('%s retries pending to execute'%pending_retries)
                retry_counts +=1
                time.sleep(60)
        except exc.SQLAlchemyError:
            logging.info('A connection Error was Raised')
            pending_retries=default_args['retries']-retry_counts
            logging.info('%s retries pending to execute'%pending_retries)
            retry_counts +=1
            time.sleep(60)

def exe_verification():
    verify_conn("moron_nacional_pampa","rio_cuarto_interamericana")

#Inintialize DAG 
with DAG(
    'OT283-34',
    description='Set Reties test for database connection for two tables from and use DAG according Alkemy Sprint01',
    #As required the DAG should execute every hour everyday
    schedule_interval=timedelta(days=1), 
    start_date=datetime(2022,8,24),
    ) as dag:
    #Using PythonOperator task are set with python callable functions
    check_db_conn_task = PythonOperator(task_id = 'check_db_connection', python_callable = exe_verification, dag=dag)
