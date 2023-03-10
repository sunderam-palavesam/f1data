from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from process_f1data import db_check, load_event_schedule, load_session_data

event_year = Variable.get("processYear")

default_args = {
    'owner': 'sunderam',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='f1dataETL',
    default_args=default_args,
    start_date=datetime(2023, 3, 5),
    schedule_interval='0 0 * * *'
) as dag:

    db_check = PythonOperator(
        task_id="db_check",
        python_callable = db_check
    )    

    load_event_schedule = PythonOperator(
        task_id="load_event_schedule",
        python_callable = load_event_schedule,
        op_kwargs={'event_year': event_year}
    )    

    load_session_data = PythonOperator(
        task_id="load_session_data",
        python_callable = load_session_data,
        op_kwargs={'event_year': event_year},    
    )  

    db_check >> load_event_schedule >> load_session_data