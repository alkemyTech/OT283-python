import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from sqlalchemy import exc, create_engine, inspect
from decouple import config

def check_db_connection():
    """
    Funcion conexión a la base de datos
        * Definimos un bucle para controlar que se ejecute solamente si los intentos son menor o igual a 5
        o existen las tablas en la base de datos
    """
    flag = True
    count = 0
    logging.info("Conectando a la base de datos")
    while flag and count < 5:
        logging.info(f"Intento N°{count}")
        try:
            # Establezco la coneccion mendiante la variable de entorno que contiene el enlace a la db postgre
            engine = create_engine(config('DB_DATA_CONNECT'))
            engine.connect()
            inspec = inspect(engine)

            # Vemos que las tablas existan para ver si se vuelve a intentar conectar o no
            if inspec.has_table("flores_comahue") and inspec.has_table('salvador_villa_maria'):
                logging.info("No existen las tablas en la base de datos")
                flag=False
            else:
                # En caso de que no se encuentren las tablas se incrementa la variable contados de control y se hace un sleep de 30 segundos entre un intento y otros para evitar delays de conexion
                count = count + 1
                time.sleep(30)
        except exc.SQLAlchemyError:
            # Se incrementa la variable contador de control en caso de encontrar un error, y de igual manera se espera 30 segundo antes del siguiente reintegro
            count = count + 1
            time.sleep(30)
    return engine

# Configuracion del login
logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%M-%D'
)

default_args = {
    'owner':'Airflow',
    'retries':5,
    'retry_delay':timedelta(seconds=30)
}

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
