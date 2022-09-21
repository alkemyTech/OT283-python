from datetime import datetime, timedelta
from venv import create

from airflow import DAG

from airflow.operators.python import PythonOperator

import time

from sqlalchemy import exc, create_engine, inspect

from decouple import config

# Se estabecen los parametros de conexion a la base de datos

db_name = config('DB_NAME')
db_user = config('DB_USER')
db_pass = config('DB_PASS')
db_host = config('DB_HOST')
db_port = config('DB_PORT')

# Se definen las propiedades del DAG
default_args = {
                'owner': 'Airflow',
                'retries': 5,
                'retry_delay': timedelta(minutes = 1)
                }

# Se crean las funciones que permitirán la conexión

def connect_param(db_name, db_user, db_pass, db_host, db_port):
    '''
    Función para establecer link de parametros de conexión a base de datos
    '''
    db_parameters = 'postgresql://' + db_user + ':' + db_pass + '@' + db_host + ':' + db_port + '/' + db_name

    return db_parameters

def connect_db():
    """
    Función para intentar establecer conexión con la base de datos.
    Se ejecuta siempre y cuando sean 5 o menos retries y las tablas existan en la base de datos
    """
    retry_count = 0
    retry_flag = True

    while retry_flag and retry_count < 5:
        try:
            # Se intenta la conexión a la base de datos con los parametros establecidos
            db_parameters = connect_param(db_name, db_user, db_pass, db_host, db_port)
            engine = create_engine(db_parameters)
            engine.connect()
            insp = inspect(engine)
            # Se verifica si las tablas existen en la base de datos. Si no, se intenta la conexión de nuevo
            if insp.has_table('flores_comahue') and insp.has_table('salvador_villa_maria'):
                retry_flag = False
            else:
            # Si no se encuentra, se aumenta el contador e intenta de nuevo
                retry_count += 1
            # Se espera 1 minuto antes de volver a intentar la conexión
                time.sleep(60)
        except exc.SQLAlchemyError:
            # Si hay un error en el intento, se aumenta el contador de retry y se vuelve al ciclo
            retry_count += 1
            time.sleep(60)
    return engine

# Se definen las propiedades del DAG
default_args = {
                'owner': 'Airflow',
                'retries': 5,
                'retry_delay': timedelta(minutes = 1)
                }
# Se define el DAG y las tareas a ejecutar
with DAG(
        'OT283-30',
        description = 'DAG para intentar establecer conexión a db para universidades B',
        default_args = default_args,
        schedule_interval = timedelta(hours = 1),
        start_date=datetime(2022, 8, 21)
        ) as dag:


    connect_db_task = PythonOperator(
                                    task_id = 'connect_db_task',
			                        python_callable = connect_db
                                    )