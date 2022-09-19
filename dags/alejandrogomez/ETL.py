import logging
from airflow import DAG
from sqlalchemy import create_engine
from datetime import timedelta, datetime
from alejandrogomez.etl_functions import DATA_DIR
from alejandrogomez.etl_functions import extract_db
from alejandrogomez.etl_functions import normalizer
from airflow.operators.python import PythonOperator
from alejandrogomez.etl_functions import upload_to_s3



logger = logging.getLogger('Universidades A')

# logs configuration
logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%M-%D',
    format='%(asctime)s- %(logger)s - %(mensaje)s'
)

# airflow default arguments configuration
default_args = {
    'owner':'Airflow',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    'dag_OT283-ETL',
    description='Configuracion DAG para Univeridades del grupo A',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,8,23),
) as dag:

    logging.info("Starting tasks")    

    # task that will be use by dags
    logging.info("Extracting data")
    extract = PythonOperator(
        task_id = 'extract',
        python_callable = extract_db,
        retries=5,
        retry_delay=timedelta(minutes=5),
        op_args=['flores.sql','villaMaria.sql'],
        dag=dag
    )
    logging.info("Transforming data")
    transform_data_to_csv = PythonOperator(
        task_id='transform',
        python_callable=normalizer,
        op_args=['flores','villaMaria'],
        dag=dag
    )

    logging.info("Uploading flores data")
    task_upload_to_s3_flores = PythonOperator(                            
                            task_id= 'upload_to_s3_flores',
                            python_callable =upload_to_s3,
                            # Parameters that will be used in the upload function, the first is file directory, second is the file name and finally the bucket name
                            op_kwargs={
                                    'filename':f'{DATA_DIR}/files/alejandrogomez/flores.txt',
                                    'key':'flores.txt',
                                    'bucket_name':'cohorte-agosto-38d749a7'
                            }
    )

    logging.info("Uploading villa maria data")
    task_upload_to_s3_villa_maria = PythonOperator(
                            task_id= 'upload_to_s3_villa_maria',
                            python_callable =upload_to_s3,
                            op_kwargs={
                                    'filename':f'{DATA_DIR}/files/alejandrogomez/villaMaria.txt',
                                    'key':'villaMaria.txt',
                                    'bucket_name':'cohorte-agosto-38d749a7'
                            }
    )

    # execution flow
    extract >> transform_data_to_csv >> [task_upload_to_s3_villa_maria,task_upload_to_s3_flores]

# if __name__ == '__main__':
    # extract_db('flores.sql','villaMaria.sql')
    # normalizer('flores','vilklaMaria')
