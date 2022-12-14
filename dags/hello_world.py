from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import psycopg2

dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

def print_hello():
    return 'Hello world from first Airflow DAG!'

def get_pandas_rows(**kwargs):
    df = pd.read_csv('/opt/airflow/datasets/weight_height_index.csv')
    print(df[['Weight', 'Height']].head(20))

    ti = kwargs['ti']
    return_value = ti.xcom_pull(task_ids='hello_task')
    print('Returned value from previous task', return_value)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
process_pandas_data = PythonOperator(task_id='process_pandas_data', python_callable=get_pandas_rows, provide_context=True, dag=dag)

hello_operator >> process_pandas_data