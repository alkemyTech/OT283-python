#   Importamos las librerias
import logging
import sys
sys.path.append("../core")

from constans import LOG_DIR
from confi import db_name, db_user, db_pass, db_host, db_port
from datetime import datetime

#   Definimos el formato de fecha 
now = datetime.now()
year = now.year
month = now.month
day = now.day
date = f"{year}-{month:02d}-{day:02d}"                                      #   Establecemos los place holders
today_date = date.format(                                               
                        year = year, 
                        month = month, 
                        day = day
                        )

#   Establecemos las configuración del log
logging.basicConfig(
                    filename = f"{LOG_DIR}/{today_date}-{db_user}.log",     #   Se generará un archivo .log el cual 
                                                                            #   tendrá en el nombre el siguiente formato
                                                                            #   %Y-%m-%d - nombre_logger(usuario de 
                                                                            #   la base de datos)
                    level = "DEBUG"
                    )

logging.info("Mensaje")                                                     #   Muestra información pertinente

#print(f"{LOG_DIR}/{today_date}-{db_user}.log")
#print(f"{LOG_DIR}/{today_date}-{db_user}: Estableciendo conexión")