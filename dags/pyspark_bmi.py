import json
import pandas as pd
import requests
import time
import logging
import psycopg2
import boto3

from pathlib import Path
from datetime import datetime
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from sqlalchemy import create_engine

LOGGER = logging.getLogger("airflow.task")

dag = DAG('pyspark_bmi', description='Registro Covid',
          schedule_interval='0 * * * *',
          start_date=datetime(2022, 11, 1), catchup=False)

def download_dataset():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0',
    }
    response = requests.get('https://drive.google.com/uc?export=download&id=1eGmLcpS4K53Bd6BSttDOdJxA7Tp0AI7P', headers=headers)
    open("/opt/airflow/datasets/owid-covid-data.csv", "wb").write(response.content)

def generate_bmi_criteria_spark():
    url = 'http://spark:6066/v1/submissions/create'

    data = { 
        "action": "CreateSubmissionRequest",
        "appArgs": [
            "file:///opt/bitnami/spark/pyspark-scripts/generate_bmi_criteria.py"
        ],
        "appResource": "/opt/bitnami/spark/pyspark-scripts/generate_bmi_criteria.py",
        "clientSparkVersion": "3.3.1",
        "environmentVariables": {
            "SPARK_ENV_LOADED": "1"
        },
        "mainClass": "org.apache.spark.deploy.SparkSubmit",
        "sparkProperties": {
            "spark.executor.memory": "1g",
            "spark.driver.memory": "1g",
            "spark.driver.cores": "1",
            "spark.driver.supervise": "false",
            "spark.app.name": "Spark REST API - Generate BMI",
            "spark.master": "spark://spark:7077",
            "spark.submit.deployMode": "cluster",
            "spark.eventLog.enabled":"true"
        }
    }

    response = requests.post(url, json=data)
    res = json.loads(response.text)
    submission_id = res['submissionId']

    LOGGER.info("Job submitted successfully. Driver id is %s" % (submission_id))

    while True:
        response = requests.get('http://spark:6066/v1/submissions/status/%s' % (submission_id))
        res = json.loads(response.text)

        if res['driverState'] == 'FINISHED':
            break
        elif res['driverState'] == 'FAILED':
            raise 'Error running job'
        
        time.sleep(1)

    LOGGER.info('Job is finished. Moving to next task')


def load_data_to_postgresql():
    data_dir = Path('/opt/airflow/datasets/registro_covid')
    df = pd.concat(pd.read_parquet(file) for file in data_dir.glob('*.parquet'))
    
    conn_string = 'postgres://postgres:postgres@postgres/airflow'
  
    db = create_engine(conn_string)
    conn = db.connect()

    df.to_sql('DIM_CIUDAD', con=conn, if_exists='replace', index=False)

    conn = psycopg2.connect(conn_string)

    conn.commit()
    conn.close()
    df.to_csv('/opt/airflow/datasets/owid-covid-data.csv')
    print(df.head(20))

def send_data_to_s3():
    s3 = boto3.resource('s3', aws_access_key_id='AKIA2TGGMNSTJACPJVKI', aws_secret_access_key='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')
    bucket = s3.Bucket('covid-19-proyecto')
    bucket.upload_file('/opt/airflow/datasets/owid-covid-data.csv', 'files/owid-covid-data.csv')

download_dataset_task = PythonOperator(task_id='download_dataset', python_callable=download_dataset, dag=dag)
generate_bmi_criteria = PythonOperator(task_id='generate_bmi_criteria', python_callable=generate_bmi_criteria_spark, dag=dag)
load_data = PythonOperator(task_id='load_data', python_callable=load_data_to_postgresql, dag=dag)
send_data_to_s3_task = PythonOperator(task_id='send_data_to_s3', python_callable=send_data_to_s3, dag=dag)

download_dataset_task >> generate_bmi_criteria
generate_bmi_criteria >> load_data
load_data >> send_data_to_s3_task
