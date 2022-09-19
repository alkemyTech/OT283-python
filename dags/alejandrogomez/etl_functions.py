import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import exc, create_engine, inspect
import os
from decouple import config
from airflow.hooks.S3_hook import S3Hook

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
            Extrat database information with queries that obtain of .sql files, then the code build csv files and save files in files folder
                Input:
                    * Arguments *args
    """
    # We get everyone argument of the function and verify if is a string argument then save it in a vrariable
    queries = [value for value in args if type(value) == str]

    if not queries:
        # If the varaible not contains nathing we send a log message
        logging.info(' No queries to process ')

    for archivo_query in queries:
        # We obtain averyone name of files to build path and build filders and files names

        # We obtain sql path and then read it and save the information into a variable
        SQL_DIR = Path(f'{DATA_DIR}/sql/alejandrogomez/{archivo_query}')
        with open(SQL_DIR,'r') as sql:
            archivo_sentencia = sql.read()

        logging.info(" Execute in progress data extraction ")

        # We connect to database and then with pandas read query and change the table into a dataframe
        engine = db_connection()
        df = pd.read_sql_query(str(archivo_sentencia),engine)
        # We create folder into especificated directory
        os.makedirs(f'{DATA_DIR}/files/alejandrogomez', exist_ok=True)
        # Convert dataframe into csv file
        # Inside the path for the name by means of a split we cut the name at the point to put only what is before the .sql as the name of the csv file.
        df.to_csv(f'{DATA_DIR}/files/alejandrogomez/{str(archivo_query).split(".")[0]}.csv', index=False)
        logging.info("CSV Creado correctamente")

def normalizer(*args):
    """
        This function extract data form csv, transform it into an specific format and save it into a txt file
            Inputs
                * Arguments files names
    """
    arguments = [value for value in args]

    # Extract names into the arguments
    for file_name in arguments:
        # Read csv file and transform it into a dataframe
        df = pd.read_csv(f'{DATA_DIR}/files/alejandrogomez/{file_name}.csv',sep=',')
        # Create and open txt file
        with open(f'{DATA_DIR}/files/alejandrogomez/{file_name}.txt','a') as data:
            # With the shape function we extract the length of the dataframe and then use it for the range of the for loop.
            for i in range(df.shape[0]):
                # At each turn of the loop we extract the information and store it in the file
                data.write((df['university'][i]).lower().strip())
                data.write(',')
                data.write(df['career'][i].lower().strip())
                data.write(',')
                data.write(df['inscription_date'][i])
                data.write(',')
                data.write(df['first_name'][i].lower().strip())
                data.write(',')
                data.write(df['last_name'][i].lower().strip())
                
                # We change the gender for the complete word
                if df['gender'][i].lower().strip() == 'M' or df['gender'][i].lower().strip() == 'M':
                    data.write('male')
                    data.write(',')
                elif df['gender'][i].lower().strip() == 'f' or df['gender'][i].lower().strip() == 'F':
                    data.write('female')
                    data.write(',')

                data.write(str(df['age'][i]))
                data.write(',')
                data.write(str(df['postal_code'][i]))
                data.write(',')
                data.write(df['location'][i].lower().strip())
                data.write(',')
                data.write(df['email'][i].lower().strip())
                data.write('\n')

def upload_to_s3(filename, key, bucket_name):
    """
        Function that takes the connection which was configured through the airflow interface
            Input:
                * File name -> File path
                * Key -> file name with extension
                * bucket_name -> bucket name configurated in s3 service                
    """
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name,acl_policy='public-read')    

def process_data_uni():
    pass
def upload_to_s3():
    pass
