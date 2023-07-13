import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner':'admin',
    'retry_delay':timedelta(minutes=5)
}

dag_spark = DAG(
    dag_id = 'project_5_spark',
    default_args=default_args,
    schedule='0 1 * * *',
    dagrun_timeout=timedelta(minutes=60),
    description='submit',
    start_date=days_ago(1)
)

start = EmptyOperator(task_id = 'start',
                      dag = dag_spark)

spark_submit = SparkSubmitOperator(
    application='/root/airflow/application/project_5.py',
    conn_id='spark-standalone',
    task_id = 'spark_submit',
    dag= dag_spark
)

end = EmptyOperator(task_id = 'end', dag=dag_spark)

start >> spark_submit >> end