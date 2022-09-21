from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

# Se creab las funciones de ETL a usar en el futuro
def extract_db():
    pass

def transform_with_pandas():
    pass

def load_to_s3():
    pass

# Se definen las propiedades del DAG
default_args = {
    'owner': 'Airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes = 5)
}

# Se define el DAG y las tareas a ejecutar
with DAG(
        'OT283-22',
        description = 'DAG para el ETL de universidades B',
        default_args = default_args,
        schedule_interval = timedelta(hours = 1),
        start_date = datetime(2022, 8, 21)
        ) as dag:



    extract = PythonOperator(
                            task_id = 'extract',
                            python_callable = extract_db,
                            retries = 5,
                            retry_delay = timedelta(minutes = 5)
                            )
    

    transform1 = PythonOperator(
                                task_id = 'transform_nacional',
			                    python_callable = transform_with_pandas
                                )
    
    transform2 = PythonOperator(
                                task_id = 'transform_salvador',
		                        python_callable = transform_with_pandas)

    load1 = PythonOperator(
                          task_id = 'load_nacional',
		                  python_callable = load_to_s3
                          )
    
    load2 = PythonOperator(
                          task_id = 'load_salvador',
		                  python_callable = load_to_s3
                          )
        

    # Se asigna el orden de la ejecución de tareas. Se agrega un transform para cada tabla de universidad.
    extract >> [transform1, transform2] 

    transform1 >> load1

    transform2 >> load2