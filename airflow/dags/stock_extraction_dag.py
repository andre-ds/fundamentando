from datetime import datetime
from utils.Utils import path_environment
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from utils.Utils import load_bucket

'''
The yfinance API always provide the information with one day of delay.
For example, if the extraction (start date) is of the 17th of january (tuesday),
the information provided is going to be of 16th (monday).
'''

EXECUTION_DATE = '{{ ds }}'
FUNDAMENTUS_RAW_STOCK = Variable.get('FUNDAMENTUS_RAW_STOCK')
FUNDAMENTUS_PRE_PROCESSED_STOCK = Variable.get('FUNDAMENTUS_PRE_PROCESSED_STOCK')

with DAG(
    dag_id='stock_extractions',
    start_date=datetime(2023, 2, 13),
    schedule_interval='10 10 * * 2-6',
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
        '--ticker_list_type', 'file',
        '--reference_date', EXECUTION_DATE,
        '--stock_tickers', 'register_2023_01_27_stock_tickers.csv']
    )

    upload_s3_raw_ticker = PythonOperator(
        task_id='upload_s3_raw_ticker_id',
        python_callable=load_bucket,
        op_kwargs={
            'bucket':f'{FUNDAMENTUS_RAW_STOCK}',
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
        '--reference_date', EXECUTION_DATE]
    )

    upload_s3_union = PythonOperator(
        task_id='upload_s3_union_id',
        python_callable=load_bucket,
        op_kwargs={
            'bucket':f'{FUNDAMENTUS_PRE_PROCESSED_STOCK}',
            'dataType':'pre-processed-stock',
            'execution_date':EXECUTION_DATE,
            'delete':True,
        }
    )

environment >> stock_extraction >> upload_s3_raw_ticker >> union_stocks >> upload_s3_union