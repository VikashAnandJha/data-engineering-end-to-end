from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def print_hello():
    return 'Hello, World!'


# Define the DAG
dag = DAG(
    'hello_world_1',
    description='Simple DAG to print Hello, World!',
    schedule_interval='@once',
    start_date=datetime(2024, 2, 18),
    catchup=False
)

# Define the task
hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Set task dependencies
hello_task
