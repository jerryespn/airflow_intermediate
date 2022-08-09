# Airflow Intermediate MySQL to hive DAG pipeline
# by jerryespn
# August 2022

import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.hive_operator import HiveOperator


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

# Hive Hook
def prepare_hive_stg():
    sql_stmt = '''
        CREATE TABLE IF NOT EXISTS categories (
            CategoryID INT,
            Picture STRING,
            Categoryname STRING,
            Description STRING,
            categoriescol STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    '''
    hive_hook = HiveServer2Hook(mysql_conn_id = 'hiveserver2_default')
    connection = hive_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql_stmt)
    cursor.close()
    connection.close()
    

#Main DAG Declaration
with DAG(dag_id='mysql_to_hive', default_args= default_args, schedule_interval='@daily', catchup=False) as dag:
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

    # Drop table in Target Hive Instance
    task_prepare_hive = PythonOperator(
        task_id = 'truncate_prepare_hive',
        python_callable = prepare_hive_stg
    )

    task_moving_data = BashOperator(
        task_id = 'task_moving_data',
        bash_command = (
            'docker cp /tmp/northwind/tmp_northwind.csv e4a9d329673e:/opt/hive/examples/files/'
        )
    )

    task_sending_hql = BashOperator(
        task_id = 'task_sending_hql',
        bash_command = (
            'docker cp /home/airflow/load_categories.hql e4a9d329673e:/opt/'            
        )
    )

    # Saving the data into Hive
    task_load_src_data = BashOperator(
        task_id = 'load_src_data',
        bash_command = (            
            'docker exec docker-hive_hive-server_1 hive -f load_categories.hql'
        )
    )    

  
task_get_mysql_data >> task_process_src_data >> task_prepare_hive >> task_moving_data >> task_sending_hql >> task_load_src_data
    



