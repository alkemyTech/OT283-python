import logging, os
import pandas as pd
from sqlalchemy import exc, create_engine

def extract(**kwargs):
    ### getting the name of the sql files
    lista_queries = [v for k, v in kwargs.items() if type(v) == str and 'sql_filename' in k]
    if not lista_queries:
        logging.info("Theres no queries to process. skipping task")
        return 0
    #logging.info(('list with the names of sql files{}'.format(lista_queries)))

    engine = kwargs['engine']
    ### For Loop to extract data from all the query names injected in the dag op_args parameter
    for query_file in lista_queries:
        with engine.connect() as con:
            with open(f"dags/sql/{query_file}") as file:
                query = file.read()              
                result_q = con.execute(query)
                #logging.info('Adding query results to a dataframe')
                df = pd.DataFrame(result_q)
                os.makedirs('files',exist_ok=True)
                df.to_csv('files/query_jujuy.csv', index=False)
                #logging.info('succesfully created the csv files')
                return 0

def transform():
    #transform the extracted data
    logging.info('transforming data')

def load():
    #load data 
    logging.info('loading data')
