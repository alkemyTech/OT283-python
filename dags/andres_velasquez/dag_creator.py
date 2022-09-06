from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging, os

from utils.database_funcs import create_engine_from_envs, check_db_connection
from utils.etl import extract, transform, load_data


#from utils.etl import extract

logger = logging.getLogger('Universidades C')

logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%M-%D'
)

def create_dag(
            dag_id,
            engine,
            query_filename,
            schedule,
            dag_number,
            default_args,
            files_folders
        ):


    dag = DAG(dag_id=f'{dag_number}_{dag_id}',
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        task_check_connection = PythonOperator(
            task_id='check_db_connection',
            python_callable=check_db_connection,
            op_kwargs={'engine':engine, 'max_retries':default_args['retries']}
        )
        task_extract_data = PythonOperator(
            task_id=f'extract_{dag_id}',
            python_callable=extract,
            op_kwargs={'engine':engine, 'query_filename':query_filename, 'files_folders':files_folders}
        )
        task_transform_data = PythonOperator(
            task_id=f'transform_{dag_id}',
            python_callable=transform,
            op_kwargs={'query_filename':query_filename, 'files_folders':files_folders}
        )
        task_load_data = PythonOperator(
            task_id=f'load_{dag_id}',
            python_callable=load_data,
            op_kwargs={
                'files_folders':files_folders,
                'filename':'{}{}_cleaned.txt'.format(files_folders['main'], query_filename[:-4]),
                'key':f'{query_filename[:-4]}_cleaned.txt',
                'bucket_name':'cohorte-agosto-38d749a7'
                }
        )
        ## instantiate worflow
        task_check_connection >> task_extract_data >> task_transform_data >> task_load_data
        

    return dag




''' Preparation for dag constructor '''
## get sql query names and a clean name for the dags
main_files_folder = 'files/'
sql_files_folder = main_files_folder + 'sql/'
sql_files = os.listdir(sql_files_folder)
## creating the sqlalchemy engine. Make sure the .env file is in the main folder
engine = create_engine_from_envs()
# build a dag for each number in range(10)
for i, filename in enumerate(sql_files):
    ## get a raw name for the dag
    raw_name = filename[6:-4] # Filename format query_{uni_name}.sql
    dag_id = 'dag_{}'.format(raw_name)

    globals()[dag_id] = create_dag(
                            dag_id = dag_id,
                            engine= engine,
                            query_filename = filename,
                            schedule= '@daily',
                            dag_number= i,
                            default_args= {
                                'owner': 'airflow',
                                'start_date': datetime(2021, 1, 1),
                                'retries': 1, #cambiar a 5
                            },
                            files_folders = {
                                'main': main_files_folder,
                                'sql': sql_files_folder
                            }
                        )