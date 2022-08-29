import logging
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
from decouple import config
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
from sqlalchemy import exc, create_engine, inspect

# Tomamos la fecha actual y el directorio raiz para armar la ruta de la carpeta que contendra los logs de el dag
ROOT_DIR = Path('../')
today = datetime.now()
# Traemos de .env las credenciales para la base de datos
DB_USER = config('DB_USER')
PASSWORD = config('PASSWORD')
HOST = config('HOST')
PORT = config('PORT')
DB_NAME = config('DB_NAME')

def db_connection():
    """
    Funcion conexión a la base de datos
        * Definimos un bucle para controlar que se ejecute solamente si los intentos son menor o igual a 5
        o existen las tablas en la base de datos
    """
    flag = False
    count = 0
    logging.info("Conectando a la base de datos")
    while not flag and count < 5:
        logging.info(f"Intento N°{count}")
        try:
            # Establezco la coneccion mendiante la variable de entorno que contiene el enlace a la db postgre
            engine = create_engine(f"postgresql://{DB_USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")
            conn = engine.connect()
            logging.info("Conexion a la base de datos realizada exitosamente")

            # Vemos que las tablas existan para ver si se vuelve a intentar conectar o no
            if engine.dialect.has_table(conn,"flores_comahue") and engine.dialect.has_table(conn, 'salvador_villa_maria'):  
                logging.info("Las tablas existen")
                flag=True
            else:
                # En caso de que no se encuentren las tablas se incrementa la variable contados de control y se hace un sleep de 30 segundos entre un intento y otros para evitar delays de conexion
                logging.info("No existen las tablas en la base de datos")
                count = count + 1
                time.sleep(30)
            
        except exc.SQLAlchemyError:
            # Se incrementa la variable contador de control en caso de encontrar un error, y de igual manera se espera 30 segundo antes del siguiente reintegro
            count = count + 1
            time.sleep(30)

    return engine

logger = logging.getLogger('Universidades A')

# Configuracion del loggs
logging.basicConfig(
    filename= f"{ROOT_DIR}../logs/retry-{today}.log",
    level=logging.INFO,
    datefmt='%Y-%M-%D',
    format='%(asctime)s- %(logger)s - %(mensaje)s'
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
        python_callable = db_connection,
        dag=dag
    )
