# Airflow Intermediate useful shell commands for airflow, databases and others
# by jerryespn
# August 2022

# Testing your DAG
airflow tasks test dag_id task_id 2022-01-01



#Docker startup HIVE
docker-compose up -d

#Docker Stop HIVE
docker-compose down

# To check Docker Container ID for your MySQL to Hive Script Load
docker ps | grep 10000