from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import  BashOperator
import sys
import os


dag_path = os.getcwd()

# from materials.python_logic.app.py import main

default_args = {
    'owner' : 'lynxkub',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    default_args = default_args,
    dag_id = 'ingestion',
    description = 'Ingest housing data from Redfin.com',
    start_date = datetime(2024,7,10),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    # file_sensor_task = FileSensor(
    #     task_id = 'file_sensor_task',
    #     filepath = r'C:\Users\jek82\OneDrive\Desktop\python_project\docker_project\docker_pipeline\materials\python_logic\app.py'
    # )
    task1 = BashOperator(
        task_id='data_ingest',
        bash_command=f'python {dag_path}/python_logic/app.py',
        
    )
    # task1 = PythonOperator(
    #     task_id = 'data_ingest',
    #     python_callable = main()
    # )

    task1