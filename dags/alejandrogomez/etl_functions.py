import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import exc, create_engine, inspect
import os
from decouple import config

# Get env variables
DB_USER = config('DB_USER')
PASSWORD = config('PASSWORD')
HOST = config('HOST')
PORT = config('PORT')
DB_NAME = config('DB_NAME')
# Get main path directory
DATA_DIR = Path(f"{os.path.dirname(os.path.realpath(__file__))}").parent.parent


def db_connection():
    """
        Function that connect with database
            * Return the engine connection 
    """
    try:
        # We use env variable to connecto to postgre database
        engine = create_engine(f"postgresql://{DB_USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")
            
    except exc.SQLAlchemyError:
        logging.info(f"Error {exc.SQLAlchemyError}")
       
    return engine


def extract_db(*args):
    """
        Function that extract information
            This function extrat database information with queries that obtain of .sql files, then the code build csv files and save files in files folder
                * function arguments *args
    """
    # We get everyone argument of the function and verify if is a string argument then save it in a vrariable
    queries = [value for value in args if type(value) == str]
    
    if not queries:
        # If the varaible not contains nathing we send a log message 
        logging.info(' No queries to process ')

    for archivo_query in queries:
        # We obtain averyone name of files to build path and build filders and files names
        
        #We obtain sql path and then read it and save the information into a variable
        SQL_DIR = Path(f'{DATA_DIR}/sql/alejandrogomez/{archivo_query}')
        with open(SQL_DIR,'r') as sql:
            archivo_sentencia = sql.read()

        logging.info(" Execute in progress data extraction ")

        # We connect to database and then with pandas read query and change the table into a dataframe
        engine = db_connection()
        df = pd.read_sql_query(str(archivo_sentencia),engine)
        # We create folder into especificated directory
        os.makedirs(f'{DATA_DIR}/files', exist_ok=True)
        # Convert dataframe into csv file
        # Dentro de la ruta para el nombre mediante un split cortamos el nombre en el punto para poner solamente loq ue esta antes del .sql como nombre del archivo csv
        df.to_csv(f'{DATA_DIR}/files/{str(archivo_query).split(".")[0]}.csv', index=False)
        logging.info("CSV Creado correctamente")


def process_data_uni():
    pass

def upload_to_s3():
    pass
