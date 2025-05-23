from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='test_git_sync',
    start_date=datetime(2025,5,1),
    schedule_interval=None,
    catchup=False
) as dag:
    BashOperator(
        task_id='hello',
        bash_command='echo "¡Hola, Airflow está leyendo mis DAGs!"'
    )