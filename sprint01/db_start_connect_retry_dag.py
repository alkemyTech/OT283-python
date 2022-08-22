# Importamos las dependencias necesarias
import logging
import time
from decouple import AutoConfig
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, exc, inspect
from sqlalchemy_utils import database_exists

#Disponibilizamos las variables de entorno 
config = AutoConfig()

db_name = config("DB_NAME")
db_user = config("DB_USER")
db_pass = config("DB_PASS")
db_host = config("DB_HOST")
db_port = config("DB_PORT")

# Configuramos los logs
now = datetime.now()
year = now.year
month = now.month
day = now.day
date = f"{day:02d}-{month:02d}-{year}"
today_date = date.format(
                        year = year, 
                        month = month, 
                        day = day
                        )
ROOT_DIR = Path("../")
logging.basicConfig(
                    filename = f"{ROOT_DIR}../logs/retry-{today_date}.log", 
                    level = "DEBUG"
                    )

# Definimos las propiedades del DAG
default_args = {
                "owner": "airflow",
                "depends_on_past": False,
                "email": ["airflow@example.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 5,
                "retry_delay": timedelta(seconds = 30),
                }

# Creamos las funciones que intervendrán en el proceso

# Enlace de conexión a la base de datos
def db_connection(db_name, db_user, db_pass, db_host, db_port):
    db_string = "postgresql://{}:{}@{}:{}/{}".format(
                                                    db_user, 
                                                    db_pass, 
                                                    db_host, 
                                                    db_port, 
                                                    db_name
                                                    )   
    return db_string
    
# Arranque de la conexión con la base de datos
def start_connct():
    retry_flag = True
    connect_flag = True
    retry_count = 0

    while connect_flag and retry_count < 5:
        logging.info("Estableciendo conexión con la base de datos")
        db_string = db_connection(
                                db_name, 
                                db_user, 
                                db_pass, 
                                db_host, 
                                db_port
                                )
        if database_exists(db_string):
            logging.info("La base de datos existe")
            connect_flag = False

        while retry_flag and retry_count < 5:
            
            try:
                engine = create_engine(db_string)
                engine.connect()
                insp_db_connect = inspect(engine)
                if insp_db_connect.has_table("uba_kenedy") and insp_db_connect.has_table("lat_sociales_cine"):
                    retry_flag = False
                else:
                    retry_count += 1
                    time.sleep(60)
            except exc.SQLAlchemyError:
                retry_count += 1
                time.sleep(60)
        
        else:
            logging.info("La base de datos no existe")
            retry_count += 1
    return engine

# Definimos el DAG
with DAG(
        "db_start_connect_retry_dag",
        default_args = default_args,
        description =  "DAG reintentar establecer la conexión con la base de datos",
        schedule_interval = timedelta(hours = 1),
        start_date = datetime(2022, 8, 23),
        tags = ["alkemy_acceleration_sptr01"],
        ) as dag:
        start_connct_task = PythonOperator(
                                            task_id = "start_connct_task", 
                                            python_callable = start_connct
                                        )