import logging, time
from sqlalchemy import exc, create_engine, inspect

def check_db_connection(max_retries=1, **kwargs):
    engine = kwargs['engine']
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
