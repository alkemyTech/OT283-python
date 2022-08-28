import pandas as pd
from datetime import datetime
from pathlib import Path

from constans import SQL_SCRIPT_DIR, DATASET_DIR
from sqlalchemy.sql import text

#   Extract Data
def get_table(table_name, engine):
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    data_path = "{category}/{year}-{month:02d}/{category}-{day:02d}-{month:02d}-{year}.csv"

    fl_path = data_path.format(
                                category = table_name, 
                                year = year, 
                                month = month, 
                                day = day
                                )
    
    csv_path = Path(f"{DATASET_DIR}/{fl_path}")
    csv_path.parent.mkdir(
                            parents=True,
                            exist_ok=True
                            )
    
    with open(f"{SQL_SCRIPT_DIR}/{table_name}.sql") as scrtp:
        sql_query = text(scrtp.read())
        df = pd.read_sql_query(
                                str(sql_query),
                                engine
                                )
        df.to_csv(
                    path_or_buf = csv_path, 
                    index=False
                    ) 
    
    return csv_path

#   Transform Data
def transform_dts(table_name, path):
    now = datetime.now()
    year = now.year
    month = now.month
    day = now.day
    data_path = "{category}/{year}-{month:02d}/transformed-{category}-{day:02d}-{month:02d}-{year}.csv"

    df = pd.read_csv(filepath_or_buffer = path)
    
    ###     Definimos las transformaciones requeridas según sea el caso
    
    fl_path = data_path.format(
                                category = table_name, 
                                year = year, 
                                month = month, 
                                day = day
                            )
    
    transformed_csv_path = Path(f"{DATASET_DIR}/{fl_path}")

    df.to_csv(
                path_or_buf = transformed_csv_path, 
                index = False
                )

    return transformed_csv_path 