from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='test_git_sync',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['prueba', 'git-sync']
) as dag:

    print_date = BashOperator(
        task_id='print_date',
        bash_command='echo "Fecha actual: $(date)"'
    )

    say_hello = BashOperator(
        task_id='say_hello',
        bash_command='echo "¡Git-Sync funciona correctamente!"'
    )

    print_date >> say_hello