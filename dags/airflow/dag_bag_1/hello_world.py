# Airflow Intermediate Job Trial User
# by jerryespn
# August 2022

#Library Import

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag_id = "dag_on_dag_bag_folder"

with DAG(dag_id=dag_id, start_date=datetime(2022, 1, 1),
         schedule_interval=None) as dag:

    def say_hello():
        print("Hola curso de Airflow Intermedio!")

    PythonOperator(task_id="say_hello", python_callable=say_hello)