from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import  BashOperator
import sys
import os


dag_path = os.getcwd()



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

    task1 = BashOperator(
        task_id='data_ingest',
        bash_command=f'python {dag_path}/python_logic/app.py',
        
    )

    task1