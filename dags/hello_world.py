from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print ("Hello, World!")

def say_goodbye():
    print("Goodbye, World!")

with DAG(
    dag_id="hello_world",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    goodbye_task = PythonOperator(
        task_id="say_goodbye",
        python_callable=say_goodbye,
    )

    hello_task >> goodbye_task  # hello runs first, then goodbye