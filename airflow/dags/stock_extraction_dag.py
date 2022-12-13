from datetime import datetime
from utils.Utils import path_environment
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from utils.Utils import load_bucket, download_s3


EXECUTION_DATE = '{{ ds }}'

with DAG(
    dag_id='stock_extractions',
    start_date=datetime(2022, 12, 7),
    schedule_interval='10 18 * * 1-5',
    catchup=True
) as dag:

    environment = PythonOperator(
        task_id='path_environment',
        python_callable=path_environment
    )

    stock_extraction = SparkSubmitOperator(
        task_id=f'stock_extractions_id',
        conn_id='spark',
        application='/opt/sparkFiles/stock_extraction.py',
        name='stock_extraction_',
        application_args=[
        '--start', EXECUTION_DATE]
    )

    upload_s3_raw_ticker = PythonOperator(
        task_id='upload_s3_raw_ticker_id',
        python_callable=load_bucket,
        op_kwargs={
            'bucket':'fundamentus-raw-stock',
            'dataType':'raw-stock',
            'execution_date':EXECUTION_DATE,
            'delete':True,
        }
    )

    union_stocks = SparkSubmitOperator(
        task_id=f'union_stocks_id',
        conn_id='spark',
        application='/opt/sparkFiles/union_stocks.py',
        name='union_stocks_',
        application_args=[
        '--execution_date', EXECUTION_DATE]
    )

    upload_s3_union = PythonOperator(
        task_id='upload_s3_union_id',
        python_callable=load_bucket,
        op_kwargs={
            'bucket':'fundamentus-pre-processed-stock',
            'dataType':'pre-processed-stock',
            'execution_date':EXECUTION_DATE,
        }
    )

environment >> stock_extraction >> upload_s3_raw_ticker >> union_stocks >> upload_s3_union