from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

with DAG(dag_id='Testdagairbyte',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
    ) as dag:

    File_to_snowfalke = AirbyteTriggerSyncOperator(
        task_id='file_to_snowflake',
        airbyte_conn_id='Testdagairbyte',
        connection_id='d9d33c5c-76ef-426d-b73f-fca72d0c809d',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )