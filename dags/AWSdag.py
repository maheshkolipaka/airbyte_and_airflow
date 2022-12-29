from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

with DAG(dag_id='AWS_dag',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
    ) as dag:

    File_to_snowfalke = AirbyteTriggerSyncOperator(
        task_id='AWSsource_to_snowflake',
        airbyte_conn_id='AWS_dag',
        connection_id='01e517fa-aadf-4b3f-8ff5-a180eee42f5f',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )