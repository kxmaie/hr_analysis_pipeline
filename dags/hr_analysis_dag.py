import psycopg2
import pandas as pd
import os
from google.cloud import bigquery
from airflow import DAG
from datetime import datetime,timedelta
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

dbt_project_path='/usr/local/airflow/dags/dbt_hr'
file_path='/usr/local/airflow/include/hr_info.csv'
def connect_to_postgres():
    
    conn=psycopg2.connect(
        host="host.docker.internal",
        port="5433",
        database="hr_data",
        user="postgres",
        password="AsA12345@@"

    )

    query='select * from public.hr_data'
    data=pd.read_sql(query,conn)
    conn.close()
    os.makedirs(os.path.dirname(file_path),exist_ok=True)
    new_data=pd.DataFrame(data)
    if os.path.exists(file_path):
        old_data=pd.read_csv(file_path)
        combine_data=pd.concat([old_data,new_data],ignore_index=False)
    else:
        combine_data=new_data
    combine_data.to_csv(file_path,index=False)

def connect_to_bigquery():
    client = bigquery.Client.from_service_account_json(
        "/usr/local/airflow/include/keys/hr-data-470216-adb894ef338f.json"
    )
    table_id='hr-data-470216.hr_data.hr_data'
    df=pd.read_csv(file_path)
    project=client.load_table_from_dataframe(df,table_id)
    project.result()
    print("done.....")

local_tz='Africa/Cairo'
with DAG (
    dag_id='hr_analysis',
    description='this dag is to load data from postgres to bigquery to analysis it',
    start_date=pendulum.datetime(2025,8,27,tz=local_tz),
    schedule='0 */2 * * *',
    catchup=False,
    dagrun_timeout=timedelta(minutes=45)
)as dag:
    task_fetch_data_from_postgres=PythonOperator(
        task_id='fetch_data',
        python_callable=connect_to_postgres
    )

    task_load_data_to_bigquery=PythonOperator(
        task_id='load_data_to_bigquery',
        python_callable=connect_to_bigquery
    )

    task_run_dbt=BashOperator(
        task_id="run_dbt",
        bash_command=f"export GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/keys/hr-data-470216-adb894ef338f.json && cd {dbt_project_path} && dbt run --profiles-dir {dbt_project_path} && dbt test --profiles-dir {dbt_project_path}"

    )
    send_email = EmailOperator(
        task_id="send_test_email",
        to="momensamer807@gmail.com",   
        subject="Airflow Test Email failed",
        html_content="<h3>please check the dag </h3>",
        trigger_rule=TriggerRule.ONE_FAILED
    )


task_fetch_data_from_postgres>>[task_load_data_to_bigquery,send_email]
task_load_data_to_bigquery>>[task_run_dbt,send_email]

