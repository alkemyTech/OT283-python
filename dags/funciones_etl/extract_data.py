import logging
import pandas as pd
from pathlib import Path
import os
from db_connections import db_connection

def extract_db(*args):
    """
        Funcion extractora de informacion:
            Se encarga de extraer la informacion de la base de datos segun las queries obtenidas de los archivos .sql, luego se construye el csv y se guarda en una carpeta files
                * Argumentos de la funcion *args
    """
    # Recorremos los argumentos de la funcion uno a uno y verificamos que sean de tipo string y luego lo guardamos en variable
    queries = [value for value in args if type(value) == str]
    
    if not queries:
        # Si no contiene nada la variable se manda un mensaje de logg
        logging.info('No hay queries para procesar')

    for archivo_query in queries:
        # Recorremos los nombres de archivos que se han pasado paera obtener las rutas y armar las carpetas y nombres de archivos
        
        # Obtenemos la ruta del archivo sql y luego lo leemos y guardamos la inforamcion en una variable
        SQL_DIR = Path(f'/../sql/{archivo_query}')
        with open(SQL_DIR,'r') as sql:
            archivo_sentencia = sql.read()

        logging.info("Ejecutando extraccion de datos")

        # Conectamos a la base de datos, luego mediante pandas leemos la query y convertimos la tabla en un dataframe
        engine = db_connection()
        df = pd.read_sql_query(str(archivo_sentencia),engine)
        # Creamos una carpeta en el directorio especificado anterior a el que nos encontramos
        os.makedirs('../files', exist_ok=True)
        # Convertimos en csv el dataframe
        # Dentro de la ruta para el nombre mediante un split cortamos el nombre en el punto para poner solamente loq ue esta antes del .sql como nombre del archivo csv
        df.to_csv(f'../files/{str(archivo_query).split(".")[0]}.csv', index=False)
        logging.info("CSV Creado correctamente")


