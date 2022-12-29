from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import time
import urllib.request
import base64
import json
import json, urllib.request
import json, pprint, requests, textwrap
from airflow.models.xcom import XCom
import boto3

def checkFile():
    # Pulls the return_value XCOM from "pushing_task"
    session = boto3.Session( aws_access_key_id='AKIA4F52A4XRV6BAEPI4', aws_secret_access_key='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')
    s3 = session.resource('s3')
    my_bucket = s3.Bucket('spotlight-s3storage')
    for my_bucket_object in my_bucket.objects.filter(Prefix='airflowtesting/'):
        x=my_bucket_object.key
    return x

def createSource(ti):
    sourceFileName = ti.xcom_pull(task_ids='filecheck')
    x= requests.post("http://L201572:8000/api/v1/sources/create",json={
        "sourceDefinitionId": "778daa7c-feaf-4db6-96f3-70fd645acc77",
        "connectionConfiguration":{
    "url": f"s3://spotlight-s3storage/{sourceFileName}",
    "format": "csv",
    "provider": {
      "storage": "S3",
      "aws_access_key_id": "AKIA4F52A4XRV6BAEPI4",
      "aws_secret_access_key": "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"
    },
    "dataset_name": sourceFileName,
    "reader_options": "{\"sep\":\",\"}"
  },
        "workspaceId": "8a1291fd-f11c-437d-802a-c5cd1baf9827",
        "name": sourceFileName
        })
    i=x.json()
    sourceID=i.pop("sourceId")
    return sourceID

default_args = {
'owner': 'airflow',
'start_date': datetime(2022, 12 ,25, 10, 00, 00),
'concurrency': 1,
'retries': 0
}

with DAG('sleeptest',
catchup=False,
default_args=default_args,
#schedule_interval='* * * * *',
schedule_interval=None,
) as dag:

    opr_hello = BashOperator(task_id='Start_operation',bash_command='echo "Staring operation"')

    checkFile_ = PythonOperator(task_id='filecheck',python_callable=checkFile,do_xcom_push=True)

    sleep = PythonOperator(task_id='sleep', python_callable= lambda: time.sleep(300))

    source = PythonOperator(task_id='source',python_callable=createSource,do_xcom_push=True)

opr_hello >> checkFile_ >> sleep >> source