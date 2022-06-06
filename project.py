from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
    

def DataOperation(ti):   
    df = pd.read_csv("/home/airflow/airflow/dags/Wuzzuf_Jobs.csv")
    df=df[df.Country.isin(['Cairo','Giza'])]
    df.to_csv('out.csv')

default_args = {
    'start_date': datetime(2022, 1, 1)
}

with DAG('etlProject', schedule_interval='@daily', default_args=default_args) as dag:
     Data_Operation = PythonOperator(
         task_id = "Data_Operation",
         python_callable = DataOperation
     )   
     Data_Operation

