import logging, time

from decouple import config
from sqlalchemy import exc, create_engine, inspect

def create_engine_from_envs():
    '''Returns an sqlalchemy engine from the env variables configuration'''
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')
    db_user = config('DB_USER')
    db_pass = config('DB_PASSWORD')
    db_name = config('DB_NAME')

    # Creating the db engine
    engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user,db_pass,db_host,db_port,db_name))
    return engine



def check_db_connection(engine, max_retries=1):
    '''check the database connection'''
    #logging.info('checking data base connection with {} retries'.format(default_args['retries']))
    retry_flag = True
    retry_count = 0
    while retry_flag and retry_count < max_retries:
        try:
            #logging.info("connecting to postgresql://{}:{}@{}:{}/{}".format(db_user,db_pass,db_host,db_port,db_name))
            conn = engine.connect()
            #logging.info('success at connecting to the database')
            # se comprueba si existen las tablas, de no ser asi se reiintenta la conexion
            if engine.dialect.has_table(conn,"jujuy_utn") and engine.dialect.has_table(conn,"palermo_tres_de_febrero"):
                #logging.info('found jujuy_utn and palermo databases')
                retry_flag = False
            else:
                #logging.info('couldnt find the tables in the database. Retrying')
                #logging.info('retrying')
                #logging.info('{} retires left'.format((max_retries - retry_count)))
                retry_count += 1
                time.sleep(60)
        except exc.SQLAlchemyError:
            #logging.info("There was a connection error")
            #logging.info('{} retires left'.format((max_retries - retry_count)))
            #se incrementa la variable de control en caso de que se produzca un error
            retry_count += 1
            #se espera 1 minuto antes de el siguiente reintento
            time.sleep(60)
