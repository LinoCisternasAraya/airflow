from datetime import timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



def Crear(): # Crea un archivo txt en la misma carpeta
    Crear = open("Hola Mundo", "w")
    Crear.write("Hola Mundo")
    Crear.close()
    print("Archivo Hola Mundo creado")


with DAG(
    'first',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    Crear_task = PythonOperator(task_id="Crear", python_callable=Crear)

