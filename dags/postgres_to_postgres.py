# Airflow Intermediate postgres to postgres DAG pipeline
# by jerryespn
# July 2022

import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'start_date' : datetime(2022,1,1)
}

# Postgres Hook
def get_src_data():
    sql_stmt = "SELECT * FROM servicecalls.servicecategory "
    pg_hook = PostgresHook(
        postgres_conn_id = 'postgres_local',
        schema = 'servicecalls'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchall()

def process_src_data(ti):
    src = ti.xcom_pull (task_ids = ['get_src_data'])
    if not src:
        raise Exception ('Theres no data!')

    src = pd.DataFrame(
            data = src[0],
            columns = ["ServiceCategoryID", "ServiceCategoryName"] 
        )

    src.to_csv(Variable.get('tmp_servicecalls_csv_location'), index = False)
    
# Main DAG Declaration    
with DAG(dag_id='postgres_to_postgres', default_args= default_args, schedule_interval='@daily', catchup=False) as dag:
    pass

    # Get the data from a table in Postgres
    task_get_src_data = PythonOperator(
        task_id='get_src_data',
        python_callable=get_src_data,
        do_xcom_push=True
    )

    # Processing Data
    task_process_src_data = PythonOperator(
        task_id = 'process_src_data',
        python_callable = process_src_data
    )

    # Truncate table in Target PostgresDB
    task_truncate_table = PostgresOperator(
        task_id = 'truncate_tgt_table',
        postgres_conn_id = 'postgres_local',
        sql = "TRUNCATE TABLE servicecalls.servicecategory_tgt"

    )

    # Saving the data into Postgres

    task_load_src_data = BashOperator(
        task_id = 'load_src_data',
        bash_command = (
            'sudo -u postgres psql -d servicecalls -U postgres -c "' 
            'COPY servicecalls.servicecategory_tgt '
            "FROM '/tmp/servicecalls/tmp_servicecalls.csv' "
            "DELIMITER ','"
            'CSV HEADER"'
        )
)

task_get_src_data >> task_process_src_data >> task_truncate_table >> task_load_src_data