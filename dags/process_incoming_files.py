from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.utils.dates import days_ago
from airflow.models import Variable

python_file_to_exec = "python /usr/local/airflow/python-scripts/example.py"

# DAG for airflow task
dag_email_recipient = "gerardoesp.lira@gmail.com"

default_args = {
   'email': dag_email_recipient,
   'email_on_failure': True,
}

dag = DAG(
    dag_id='process_incoming_files',
    default_args=default_args,
    start_date=days_ago(2),
    schedule_interval='@hourly'
)

file_processor = BashOperator(
    task_id='process_incoming_files',
    bash_command=python_file_to_exec,
    dag=dag
)

file_processor