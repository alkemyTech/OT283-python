import pandas as pd
import time
import logging
from datetime import datetime, timedelta

from confi import db_name, db_user, db_pass, db_host, db_port
from constans import LOG_DIR, SCHEMA_NAME

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from sqlalchemy import exc, create_engine, inspect
from sqlalchemy_utils import database_exists
from sqlalchemy.sql import text


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

logging.basicConfig(
                    filename = f"{LOG_DIR}/{today_date}-{db_user}.log", 
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


def start_connct(
                db_name = db_name, 
                db_user = db_user, 
                db_pass = db_pass, 
                db_host = db_host, 
                db_port = db_port, 
                schema_name = SCHEMA_NAME
                ):
    db_string = "postgresql://{}:{}@{}:{}/{}".format(
                                                    db_user, 
                                                    db_pass, 
                                                    db_host, 
                                                    db_port, 
                                                    db_name
                                                    )
    retry_flag = True
    connect_flag = True
    retry_count = 0

    while connect_flag and retry_count < 5:
        logging.info("Estableciendo conexión con la base de datos")
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
                        logging.info("alguno de los esquemas no existe, reintentando")
                        retry_count += 1
                        time.sleep(60)
                except exc.SQLAlchemyError:
                    retry_count += 1
                    time.sleep(60)

        else:
            logging.info("La base de datos no existe")
            retry_count += 1
    return engine

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
