import airflow
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import numpy as np
import logging


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
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
        }
# Dictionary with dtypes for each columns
dtype_dict={'university':str, 'career':str, 'firstname':str, 'lastname':str,
       'gender':str, 'age':int, 'location':str, 'postal_code':str, 'email':str}


def normalizer(*args):
    """
    this fuction is to normalize each input file for required style
    """
    arguments = [value for value in args]

    # Extract names into the arguments
    for file_name in arguments:
        logging.info('start normalizing file : %s'%file_name)
        # Read csv file and transform it into a dataframe
        df = pd.read_csv(f'{file_name}.csv',parse_dates=True,infer_datetime_format=True,dtype=dtype_dict)
        df.university=df.university.str.replace('-',' ')
        df.university=df.university.str.lower()
        df.career=df.career.str.replace('-',' ')
        df.career=df.career.str.lower()
        df.inscription_date=pd.to_datetime(df.inscription_date)
        df.firstname=df.firstname.str.lower()
        df.lastname=df.lastname.str.lower()
        df.age=np.int64(df.age)
        df.location.str.replace('-',' ')
        df.gender=df.gender.replace({'M':'Male','F':'Female'})
        df.to_csv(f'{file_name}.txt',index=None,sep=' ')
        logging.info('finished normalizing file : %s'%file_name)



with DAG(
    'OT283-66',
    description='pandas normalization',
    #As required the DAG should execute every hour everyday
    schedule_interval=timedelta(days=1), 
    start_date=datetime(2022,9,4)
    ) as dag:
    #Using PythonOperator task are set with python callable functions
    norm_task= PythonOperator(task_id = 'norm_df_with_pandas', python_callable =normalizer,op_args=['r_query1_moron_nacional_pampa','r_query2_rio_cuarto_interamericana'], dag=dag)

    norm_task