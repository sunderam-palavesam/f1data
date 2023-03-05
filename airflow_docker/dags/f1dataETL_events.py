from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from process_f1data_events import db_check, load_event_schedule

event_year = Variable.get("processYear")

default_args = {
    'owner': 'sunderam',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='f1dataETL_events',
    default_args=default_args,
    start_date=datetime(2023, 3, 4),
    schedule_interval='0 0 * * *'
) as dag:

    db_check = PythonOperator(
        task_id="db_check",
        python_callable = db_check
    )    

    load_event_schedule = PythonOperator(
        task_id="load_event_schedule",
        python_callable = load_event_schedule,
        op_kwargs={'event_year': event_year},
    )        

    db_check >> load_event_schedule