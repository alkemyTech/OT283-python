from cmath import log
import airflow
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine,exc,inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from dotenv import dotenv_values

# setting up the login


logging.basicConfig(level=logging.INFO, datefmt='%Y-%M-%D')

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
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }


def check_db_and_extract(user,passwd,host,port,db,q1,q2):
    table1="moron_nacional_pampa"
    table2="rio_cuarto_interamericana"

    
    
    logging.info('initialized function to get engine for data base connection')
    url = "postgresql://%s:%s@%s:%s/%s"%(user,passwd,host,port,db)
    logging.info('%s'%url)
    if not database_exists(url):
        create_database(url)
        logging.info('database connection did not exist... creating new connection')
    engine = create_engine(url,pool_size=5, echo=False)
    logging.info(f'created connection to url')

    logging.info('Connection to database checking process with {} retries'.format(default_args['retries']))
    retry_flag = True
    retry_counts = 0
    while retry_flag and retry_counts < default_args['retries']:
        try:
            engine.connect()
            logging.info('successful connection to database')
            insp = inspect(engine)
            # Check if tables exist at the database server, if not success retry the connection
            if insp.dialect.has_table(engine.connect(),table1) and insp.dialect.has_table(engine.connect(),table2):
                logging.info('tables %s and %s found on database'%(table1,table2))
                retry_flag = False
            else:
                logging.info('The tables %s and %s were not found in the databse...Retrying'%(table1,table2))
                logging.info('Retrying...')
                pending_retries=default_args['retries']-retry_counts
                logging.info('%s retries pending to execute'%pending_retries)
                retry_counts +=1
                TimestampFromTicks.sleep(60)
        except exc.SQLAlchemyError:
            logging.info('Retrying...')
            pending_retries=default_args['retries']-retry_counts
            logging.info('%s retries pending to execute'%pending_retries)
            retry_counts +=1
            TimedRotatingFileHandler.sleep(60)
    
    
    logging.info('Extracting...')
    queries_list = [q1,q2]
    logging.info('Input sql files %s and %s found ....'%(queries_list[0],queries_list[1]))
    
    logging.info('Opening file 1: %s checking process with %s retries'%(q1,default_args['retries']))
    retry_flag = True
    retry_counts = 0
    while retry_flag and retry_counts < default_args['retries']:  
        try:
            with open('dags/%s'%q1,encoding='utf-8') as f:
                query1=f.read()
            f.close()
            logging.info('file1 opened')
            #query1 = u"\n".join(lines)
        except exc.SQLAlchemyError:
            logging.info('Retrying...')
            pending_retries=default_args['retries']-retry_counts
            logging.info('%s retries pending to execute'%pending_retries)
            retry_counts +=1
            TimedRotatingFileHandler.sleep(60)
    
    logging.info('Opening file 2: %s checking process with %s retries'%(q2,default_args['retries']))
    retry_flag = True
    retry_counts = 0
    while retry_flag and retry_counts < default_args['retries']:  
        try:
            with open('dags/%s'%q2,encoding='utf-8') as f:
                query2=f.read()
            f.close()
            #query2 = u"\n".join(lines)
        except exc.SQLAlchemyError:
            logging.info('Retrying...')
            pending_retries=default_args['retries']-retry_counts
            logging.info('%s retries pending to execute'%pending_retries)
            retry_counts +=1
            TimedRotatingFileHandler.sleep(60)

    logging.info('files read successfully')

    
    R_query1 = pd.read_sql(query1, con=engine)
    logging.info('DataFrame with resulting query 1')
    R_query2 = pd.read_sql(query2, con=engine)
    logging.info('DataFrame with resulting query 2')

    current = os.getcwd()
    pathfile = os.path.join(current, 'files')
    try:
        os.mkdir(pathfile)
    except:
        pass

    
    filename = "%s.csv" % (table1)
    path_filename1 = os.path.join(pathfile, filename)
    R_query1.to_csv(path_filename1,index=False)
    filename = "%s.csv" % (table2)
    path_filename2 = os.path.join(pathfile, filename)
    R_query2.to_csv(path_filename2,index=False)

def run_ext():
    check_db_and_extract('alkymer2','Alkemy23','training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com','5432','training','quer1.sql','quer2.sql')

def transform():
    logging.info('Transforming extracted data...')
    
def load():
    logging.info('Loading data...')


with DAG(
    'OT283-50',
    description='Define a dummy DAG for Alkemy Sprint1',
    #As required the DAG should execute every hour everyday
    schedule_interval=timedelta(hours=1), 
    start_date=datetime(2022,8,24),
    ) as dag:
    #Using PythonOperator task are set with python callable functions
    extract_task = PythonOperator(task_id = 'extract', python_callable = run_ext)
    transform_task = PythonOperator(task_id = 'transform', python_callable= transform)
    load_task = PythonOperator(task_id = 'load', python_callable= load)

    logging.info('Executing scheduled tasks...')
    extract_task >> transform_task >> load_task
