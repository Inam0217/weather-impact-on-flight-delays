from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flight_weather_etl",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl","weather","flights"],
) as dag:

    flights = BashOperator(
        task_id="ingest_flights",
        bash_command="python etl/flights/extract_transform_load.py --csv data/raw/flights_2018.csv",
        cwd=os.getenv("PROJECT_ROOT", "/opt/airflow")
    )

    weather_jfk = BashOperator(
        task_id="ingest_weather_jfk",
        bash_command="python etl/weather/extract_transform_load.py --date 2018-01-01 --airport JFK",
        cwd=os.getenv("PROJECT_ROOT", "/opt/airflow")
    )

    flights >> weather_jfk
