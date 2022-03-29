import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

download_files_cmd = 'sh /usr/local/bin/download_files'
spark_submit_cmd = 'spark-submit --num-executors 3 --executor-cores 3 --executor-memory 2G' \
                   ' /usr/local/bin/movie_ratings_ingestion.py --job_name movie_ratings --action ingest'

with DAG(
    dag_id='movie_ratings',
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
) as dag:
    start = DummyOperator(
        task_id='Start',
    )
    download_files = BashOperator(
        task_id='download_data',
        bash_command=download_files_cmd,
    )
    ingest_ratings = BashOperator(
        task_id='ingest_ratings',
        bash_command=spark_submit_cmd,
    )
    end = DummyOperator(
        task_id='End',
    )
    start >>  download_files >> ingest_ratings >> end

if __name__ == "__main__":
    dag.cli()