# Airflow Intermediate Job Trial User
# by jerryespn
# August 2022

#Library Imports
import os
from airflow.models import DagBag
dags_dirs = ['~/airflow/dag_bag_1', '~/airflow/dag_bag_2']

for dir in dags_dirs:
    dag_bag = DagBag(os.path.expanduser(dir))

    if dag_bag:
        for dag_id, dag  in dag_bag.dags.items():
            globals()[dag_id] = dag