
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import urllib.request
import base64
import json
import json, urllib.request
import json, pprint, requests, textwrap
from airflow.models.xcom import XCom
import boto3

username='airbyte'
password='airbyte'

def checkFile():
    # Pulls the return_value XCOM from "pushing_task"
    session = boto3.Session( aws_access_key_id='AKIA4F52A4XRV6BAEPI4', aws_secret_access_key='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')
    s3 = session.resource('s3')
    my_bucket = s3.Bucket('spotlight-s3storage')
    for my_bucket_object in my_bucket.objects.filter(Prefix='uploadfiles/'):
        x=my_bucket_object.key
    return x 

def createSource(ti):
    sourceFileName = ti.xcom_pull(task_ids='filecheck')
    x= requests.post("http://L201572:8000/api/v1/sources/create",auth=(username,password),json={
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

def createdes(ti):
    y=requests.post(url="http://L201572:8000/api/v1/destinations/create",auth=(username,password),json={
  "workspaceId": "8a1291fd-f11c-437d-802a-c5cd1baf9827",
  "name": "Snowfalke",
  "destinationDefinitionId": "424892c4-daac-4491-b35d-c6688ba547ba",
  "connectionConfiguration": {
    "host": "ax46812.ap-south-1.aws.snowflakecomputing.com",
    "role": "ACCOUNTADMIN",
    "schema": "airbyte_load_dag",
    "database": "DEMO",
    "username": "Admin",
    "warehouse": "COMPUTE_WH",
    "credentials": {
      "password": "Admin@123"
    },
    "loading_method": {
      "method": "Internal Staging"
    }
    }
    })
    j=y.json()
    destination=j.pop("destinationId")
    return destination

def connection(ti):
  sourceconnid = ti.xcom_pull(task_ids='source')
  desconnid= ti.xcom_pull(task_ids='destination')
  sourceFileName = ti.xcom_pull(task_ids='filecheck')
  z=requests.post(url='http://L201572:8000/api/v1/connections/create',auth=(username,password),json={
   "name": "dag_test_conn",
    "namespaceDefinition": "source",
    "namespaceFormat": "${SOURCE_NAMESPACE}",
    "prefix": "",
    "sourceId": sourceconnid,
    "destinationId": desconnid,
      "operationIds": [
    "9d24a761-0703-46d6-8948-90c37ed97748"
      ],
    "syncCatalog": {
      "streams": [
       {
        "stream": {
          "name": sourceFileName,
          "jsonSchema": {
          },
          "supportedSyncModes": [
            "full_refresh"
          ],
          "defaultCursorField": [],
          "sourceDefinedPrimaryKey": [],
          "namespace": sourceFileName
        },
        "config": {
          "syncMode": "full_refresh",
          "cursorField": [],
          "destinationSyncMode": "append",
          "primaryKey": [],
          "aliasName": sourceFileName,
          "selected": True,
          "fieldSelectionEnabled": False
         }
        }
      ]
    },
      "scheduleType": "manual",
      "status": "active",
      "geography": "auto",
      "notifySchemaChanges": True,
     "nonBreakingChangesPreference": "ignore"
    })
  k=z.json()
  connID=k.pop('connectionId')
  return connID

def discover_schema(ti):
  connid=ti.xcom_pull(task_ids='create_conncetion')
  sourceconnid = ti.xcom_pull(task_ids='source')
  a=requests.post(url="http://L201572:8000/api/v1/sources/discover_schema", auth=(username,password),json={
  "sourceId": sourceconnid,
  "connectionId": connid,
  "disable_cache": False
  })
  l=a.json()
  catlogid=l.pop('catalogId')
  k=l.pop('catalog')
  stream=k['streams'][0]['stream']
  ti.xcom_push(key='stream_schema',value=stream)
  return catlogid

def update_conn(ti):
  catlogid= ti.xcom_pull(task_ids='discover_schema')
  connid=ti.xcom_pull(task_ids='create_conncetion')
  sourceFileName = ti.xcom_pull(task_ids='filecheck')
  stream=ti.xcom_pull(key='stream_schema',task_ids='discover_schema')
  requests.post(url='http://L201572:8000/api//v1/connections/update', auth=(username,password),json={
  "connectionId": connid,
  "namespaceDefinition": "source",
  "namespaceFormat": "${SOURCE_NAMESPACE}",
  "name": "dag_test_update",
  "prefix": "",
  "operationIds": [
    "9d24a761-0703-46d6-8948-90c37ed97748"
  ],
  "syncCatalog": {
    "streams": [
      {
        "stream": stream,
        "config": {
          "syncMode": "full_refresh",
          "cursorField": [
          ],
          "destinationSyncMode": "append",
          "primaryKey": [
          ],
          "aliasName": sourceFileName,
          "selected": True,
          "fieldSelectionEnabled": False
        }
      }
    ]
  },
  "scheduleType": "manual",
  "status": "active",
  "sourceCatalogId": catlogid,
  "geography": "auto",
  "notifySchemaChanges": False,
  "nonBreakingChangesPreference": "ignore",
  "breakingChange": False
  }
  )

def syncconn(ti):
  connid=ti.xcom_pull(task_ids='create_conncetion')
  requests.post(url='http://L201572:8000/api/v1/connections/sync',auth=(username,password),json={
  "connectionId": connid
})

default_args = {
'owner': 'airflow',
'start_date': datetime(2022, 12 ,25, 10, 00, 00),
'concurrency': 1,
'retries': 0
}

with DAG('Airbyte',
catchup=False,
default_args=default_args,
#schedule_interval='* * * * *',
schedule_interval=None,
) as dag:

    opr_hello = BashOperator(task_id='Start_operation',bash_command='echo "Staring operation"')

    checkFile_ = PythonOperator(task_id='filecheck',python_callable=checkFile,do_xcom_push=True)

    source = PythonOperator(task_id='source',python_callable=createSource,do_xcom_push=True)

    destination= PythonOperator(task_id='destination',python_callable=createdes,do_xcom_push=True)

    create_connection= PythonOperator(task_id='create_conncetion',python_callable=connection,do_xcom_push=True)

    dis_schema= PythonOperator(task_id='discover_schema',python_callable=discover_schema,do_xcom_push=True)

    update_connection= PythonOperator(task_id='update_connect', python_callable=update_conn)

    sync_connection= PythonOperator(task_id='sync_connection',python_callable=syncconn)


opr_hello >> checkFile_ >> source >> destination >> create_connection >> dis_schema >> update_connection >> sync_connection

