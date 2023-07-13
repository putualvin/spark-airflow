from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 7, 5),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'basic_airflow_script_2',
    default_args=default_args,
    description='A basic Airflow script',
    schedule=timedelta(days=1)
)

task1 = BashOperator(
    task_id='task1',
    bash_command='echo "Task 1 executed"',
    dag=dag
)

task2 = BashOperator(
    task_id='task2',
    bash_command='echo "Task 2 executed"',
    dag=dag
)

task3 = BashOperator(
    task_id='task3',
    bash_command='echo "Task 3 executed"',
    dag=dag
)

task1 >> task2 >> task3
