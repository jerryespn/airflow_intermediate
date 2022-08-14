# Airflow Intermediate Job Trial User
# by jerryespn
# August 2022

import sys

sys.path.insert(0, '/home/airflow/airflow/custom')

from datetime import datetime, timedelta
import airflow
from airflow import DAG
from MySqlToPostgreOperator import MySqlToPostgreOperator

dag = DAG(dag_id="job_trial_user", start_date=datetime.today() - timedelta(days=1), schedule_interval="0 */4 * * *", concurrency=100
)

start = MySqlToPostgreOperator(
    task_id=f"start",
    sql="select * from orders " 
    "where OrderDate BETWEEN '{start_date}' "
    "AND '{end_date}'",
    target_table='servicecalls.orders',
    identifier='OrderID',
    dag=dag,
)