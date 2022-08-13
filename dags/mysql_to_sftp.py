# Airflow Intermediate MySQL to SFTP DAG pipeline
# by jerryespn
# August 2022

import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.sftp.operators.sftp import SFTPOperation,SFTPOperator

default_args = {
    'start_date' : datetime(2022,1,1)
}

#mySQL Hook
def get_mysql_data():
  sql_stmt = "SELECT * FROM northwind.categories"
  mysql_hook = MySqlHook(mysql_conn_id = 'mySQL_local', schema = 'northwind')
  connection = mysql_hook.get_conn()
  cursor = connection.cursor()
  cursor.execute(sql_stmt)
  return cursor.fetchall()
  
def process_src_data(ti):
    src = ti.xcom_pull (task_ids = ['get_mysql_data'])
    if not src:
        raise Exception ('Theres no data!')

    src = pd.DataFrame(
            data = src[0],
            columns = ["CategoryID", "Picture", "CategoryName", "Description", "categoriescol"] 
        )

    src.to_csv(Variable.get('tmp_northwind_csv_location'), index = False)
    
# Main DAG Declaration    
with DAG(dag_id='mysql_to_sftp', default_args= default_args, schedule_interval='@daily', catchup=False) as dag:
    pass  

    # Get the data from a table in MySQL
    task_get_mysql_data = PythonOperator(
        task_id='get_mysql_data',
        python_callable=get_mysql_data,
        do_xcom_push=True
    )

    # Processing Data
    task_process_src_data = PythonOperator(
        task_id = 'process_src_data',
        python_callable = process_src_data
    )

    task_sftp_load_data = SFTPOperator(
        task_id = 'sftp_load_data',
        ssh_conn_id = 'sftp_default',
        remote_filepath = '/home/airflow/sftp_airflow/northwind.csv',
        local_filepath = '/tmp/northwind/tmp_northwind.csv',
        operation = 'put',
        create_intermediate_dirs = True
    )

task_get_mysql_data >> task_process_src_data >> task_sftp_load_data