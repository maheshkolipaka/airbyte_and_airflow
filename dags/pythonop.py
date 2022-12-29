from operator import imod
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.wasb_hook import WasbHook
from datetime import datetime

 

#file_path ="/home/sshuser/test.txt"

 

def check_connection():

 

    return('hello world')

 

def respond():

 

    return 'Task ended'

 

default_args = {

 

'owner': 'airflow',

 

'start_date': datetime(2022, 9 ,19, 10, 00, 00),

 

'concurrency': 1,

 

'retries': 0

 

}

 

with DAG('airflow_file_uploader',

 

catchup=False,

 

default_args=default_args,
#schedule_interval='* * * * *',

 

schedule_interval=None,

 

) as dag:

 

    opr_hello = BashOperator(task_id='Start_operation',bash_command='echo "Staring operation"')

 

    check_connection_opr = PythonOperator(task_id='connection',python_callable=check_connection)

 

    opr_respond = PythonOperator(task_id='task_end',python_callable=respond)

 

opr_hello >> check_connection_opr  >> opr_respond