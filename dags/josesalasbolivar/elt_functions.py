import pandas as pd
from datetime import datetime
from pathlib import Path

from constans import SQL_SCRIPT_DIR, DATASET_DIR
from sqlalchemy.sql import text

from airflow.hooks.S3_hook import S3Hook

#   Extract Data


def get_table(
        table_name: str,
        engine: object) -> str:
    """Esta función recibe el nombre de la tabla y la conexión a la base de datos, para ejecutar 
    una cosulta sobre dicha tabla y devolverla como un archivo .csv.
    Args:
        table_name (str): nombre de la tabla sobre la cual se hará la consulta.
        engine (object): conexión con la base de datos

    Returns:
        str: ruta al archivo .csv generado
    """
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    data_path = "{category}/{year}-{month:02d}/{category}-{day:02d}-{month:02d}-{year}.csv"

    fl_path = data_path.format(
        category=table_name, year=year,
        month=month, day=day)

    csv_path = Path(f"{DATASET_DIR}/{fl_path}")
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(f"{SQL_SCRIPT_DIR}/{table_name}.sql") as scrtp:
        sql_query = text(scrtp.read())
        df = pd.read_sql_query(
            str(sql_query),
            engine)
        df.to_csv(path_or_buf=csv_path, index=False)

    return csv_path

#   Transform Data


def transform_dts(
        table_name: str,
        path: str) -> str:
    """Esta función recibe el nombre de la tabla y la ruta al archivo .csv correspondiente, 
    para cargarlo como dataframe y realizar las transformaciones pertinentes.

    Args:
        table_name (str): nombre de la fuente de datos sobre la cual se hará la transformación.
        path (str): ruta al archivo .csv que contiene los datos.

    Returns:
        str: ruta al archivo .txt que contiene los datos transformados
    """
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    data_path = "{category}/{year}-{month:02d}/transformed-{category}-{day:02d}-{month:02d}-{year}.txt"

    df = pd.read_csv(filepath_or_buffer=path)

    # definimos las transformaciones requeridas según sea el caso, mediante funciones anidadas

    def normalization_function(df01: object) -> object:
        """Esta es una función anidad, la cual recibe un objeto dataframe y aplica una serie 
        de transformaciones sobre el, para normalizarlo
        Args:
            df01 (object): dataframe sobre el cual se realizarán las transformaciones requeridas
        para normalizarlo
        Returns:
            object: dataframe normalizado
        """
        # instanciamos el df de dimensiones (con los codigos postales y locaciones) para
        # normalizar
        file_path02 = "../files/codigos_postales.csv"
        sep02 = ","
        enc02 = "utf-8"
        df02 = pd.read_csv(
            filepath_or_buffer=file_path02,
            sep=sep02,
            encoding=enc02)
        # se cuentan con dos fuentes de datos: lat_social_uni y jfk_uni, ambas requieren enfoques
        # diferentes
        if table_name == "lat_social_uni":
            def lower_dash(i: str) -> object:
                """Esta función normaliza: str minúsculas, sin espacios extras, ni guiones a 
                las columnas "university", "career", "first_name", "last_name".
                Args:
                    i (str): nombre de la columna sobre la cual se hará la normalización.
                Returns:
                    object: dataframe modificado.
                """
                df01[f"{i}"] = df01[f"{i}"].str.lower()
                df01[f"{i}"] = df01[f"{i}"].str.replace(pat="-",
                                                        repl=" ")

            def date_format() -> object:
                """Esta función normaliza: str formato %Y-%m-%d a la columna "inscription_date"
                Returns:
                    object: dataframe modificado.
                """
                df01["inscription_date"] = pd.to_datetime(df01.inscription_date,
                                                          format='%Y-%m-%d')

            def discretize() -> object:
                """Esta función normaliza: str seleccion dicotomica (male, female) a la columna 
                "gender".
                Returns:
                    object: dataframe modificado.
                """
                # gender m to male
                df01["gender"].loc[df01["gender"] == "M"] = "male"
                # gender f to female
                df01["gender"].loc[df01["gender"] == "F"] = "female"

            def age_convert() -> object:
                """Esta función convierte los días desde el nacimiento en la edad de la persona.
                Returns:
                    object: dataframe modificado.
                """
                df01["age"] = df01["age"].str.replace(pat="days",
                                                      repl="").astype("int")
                df01["age"] = (df01["age"] / (365)).astype(dtype="int")

            def location_nomenclature() -> object:
                """Esta función normaliza como: str minúscula sin espacios extras, ni guiones 
                las columnas "location", "postal_code".
                Returns:
                    object: dataframe modificado.
                """
                df01["postal_code"] = df01["postal_code"].astype(dtype="int")
                # location: str minúscula sin espacios extras, ni guiones
                df01["location"] = df01["location"].str.lower()

            def email_lower_dash() -> object:
                """Esta función normaliza como: str minúsculas, sin espacios extras la columna
                "email"
                Returns:
                    object: dataframe modificado.
                """
                df01["email"] = df01["email"].str.lower()
                df01["email"] = df01["email"].str.replace(pat=" ",
                                                          repl="")
            # lat_soci_uni
            # renombramos la columna de df02, para que exista coincidencia al hacer hacer "merge"
            renamed_columns = {
                "localidad": "location"
            }
            df02.rename(columns=renamed_columns,
                        inplace=True)
            # reemplazamos los guiones por espacios
            df01["location"] = df01["location"].str.replace(pat="-",
                                                            repl=" ")
            # mergeamos los df's
            lft = df01
            rght = df02
            join = "inner"
            column_name = "location"
            df01 = pd.merge(left=lft,
                            right=rght,
                            how=join,
                            on=column_name)
            # reordenamos las columnas del df meregeado
            columns = [
                "university",
                "career",
                "inscription_date",
                "first_name",
                "last_name",
                "gender",
                "age",
                "codigo_postal",
                "location",
                "email"
            ]
            df01 = df01.reindex(columns=columns)
            # renombramos las columnas de df mergeado
            renamed_columns = {
                "codigo_postal": "postal_code"
            }
            df01.rename(columns=renamed_columns, inplace=True)
            # instrucciones de transformacion
            for i in df01:
                if i in ["university", "career", "first_name", "last_name"]:
                    print(f"aplicar aspectos formales a: {i}")
                    lower_dash(i)
                elif i == "inscription_date":
                    print(f"aplicar formato de fecha a: {i}")
                    date_format()
                elif i == "gender":
                    print(f"discretizar: {i}")
                    discretize()
                elif i == "age":
                    print(f"calcular edad a partir de los días de nacido: {i}")
                    age_convert()
                elif i == "location":
                    location_nomenclature()
                    print(f"transfotmando: {i}")
                elif i == "email":
                    print(f"transformar: {i}")
                    email_lower_dash()
        elif table_name == "jfk_uni":
            def lower_dash(i: str) -> object:
                """Esta función normaliza: str minúsculas, sin espacios extras, ni guiones a 
                las columnas "university", "career", "first_name", "last_name".
                Args:
                    i (str): nombre de la columna sobre la cual se hará la normalización.
                Returns:
                    object: dataframe modificado.
                """
                df01[f"{i}"] = df01[f"{i}"].str.lower()
                df01[f"{i}"] = df01[f"{i}"].str.replace(pat="-",
                                                        repl=" ")

            def date_format() -> object:
                """Esta función normaliza: str formato %Y-%m-%d a la columna "inscription_date"
                Returns:
                    object: dataframe modificado.
                """
                df01["inscription_date"] = pd.to_datetime(df01.inscription_date,
                                                          format='%Y-%m-%d')

            def discretize() -> object:
                """Esta función normaliza: str seleccion dicotomica (male, female) a la columna 
                "gender".
                Returns:
                    object: dataframe modificado.
                """
                # gender m to male
                df01["gender"].loc[df01["gender"] == "m"] = "male"
                # gender f to female
                df01["gender"].loc[df01["gender"] == "f"] = "female"

            def age_convert() -> object:
                """Esta función convierte los días desde el nacimiento en la edad de la persona.
                Returns:
                    object: dataframe modificado.
                """
                df01["age"] = df01["age"].str.replace(pat="days",
                                                      repl="").astype("int")
                df01["age"] = (df01["age"] / (365)).astype(dtype="int")

            def post_code_nomenclature() -> object:
                """Esta función normaliza como: str minúscula sin espacios extras, ni guiones 
                las columnas "location", "postal_code".
                Returns:
                    object: dataframe modificado.
                """
                df01["postal_code"] = df01["postal_code"].astype(dtype="int")
                # location: str minúscula sin espacios extras, ni guiones
                df01["location"] = df01["location"].str.lower()

            def email_lower_dash() -> object:
                """Esta función normaliza como: str minúsculas, sin espacios extras la columna
                "email"
                Returns:
                    object: dataframe modificado.
                """
                df01["email"] = df01["email"].str.lower()
                df01["email"] = df01["email"].str.replace(pat=" ",
                                                          repl="")
            # jfk_uni
            # renombramos la columna de df02, para que exista coincidencia al hacer hacer "merge"
            renamed_columns = {
                "codigo_postal": "postal_code"
            }
            df02.rename(columns=renamed_columns,
                        inplace=True)
            # mergeamos los df's
            lft = df01
            rght = df02
            join = "inner"
            column_name = "postal_code"
            df01 = pd.merge(left=lft,
                            right=rght,
                            how=join,
                            on=column_name)
            # reordenamos las columnas del df meregeado
            columns = [
                "university",
                "career",
                "inscription_date",
                "first_name",
                "last_name",
                "gender",
                "age",
                "postal_code",
                "localidad",
                "email"
            ]
            df01 = df01.reindex(columns=columns)
            # renombramos las columnas de df mergeado
            renamed_columns = {
                "localidad": "location"
            }
            df01.rename(columns=renamed_columns, inplace=True)
            # instrucciones de normalización
            for i in df01:
                if i in ["university", "career", "first_name", "last_name"]:
                    print(f"aplicar aspectos formales a: {i}")
                    lower_dash(i)
                elif i == "inscription_date":
                    print(f"aplicar formato de fecha a: {i}")
                    date_format()
                elif i == "gender":
                    print(f"discretizar: {i}")
                    discretize()
                elif i == "age":
                    print(f"calcular edad a partir de los días de nacido: {i}")
                    age_convert()
                elif i == "location":
                    post_code_nomenclature()
                    print(f"transfotmando: {i}")
                elif i == "email":
                    print(f"transformar: {i}")
                    email_lower_dash()
        return df01

    df = normalization_function(df01=df)

    fl_path = data_path.format(
        category=table_name, year=year,
        month=month, day=day)

    transformed_path_dict = Path(f"{DATASET_DIR}/{fl_path}")

    df.to_csv(path_or_buf=transformed_path_dict, index=False)

    return transformed_path_dict


def load_data_to_s3(filename: str,
                    table_name: str,
                    bucket_name: str) -> None:
    """Esta función realiza la carga de los datos normalizados y almacenados en un .txt y los 
    carga al AS3 bucket.

    Args:
        filename (str): ruta al archivo.
        table_name (str): nombre de la tabla correspondiente al archivo.
        bucket_name (str): nombre de la instancia configurada en AWS que recibirá los datos.
    """
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    file_name_format = "transformed-{category}-{day:02d}-{month:02d}-{year}.txt"
    key = file_name_format.format(category=table_name,
                                  year=year,
                                  month=month, day=day)
    hook = S3Hook("s3_conn")
    hook.load_file(filename=filename, key=key,
                   bucket_name=bucket_name, acl_policy='public-read')
