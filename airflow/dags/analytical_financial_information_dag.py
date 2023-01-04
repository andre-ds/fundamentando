from datetime import datetime
from utils.Utils import path_environment
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from utils.Utils import load_bucket


EXECUTION_DATE = '{{ ds }}'
FUNDAMENTUS_PRE_PROCESSED_ITR = Variable.get('FUNDAMENTUS_PRE_PROCESSED_ITR')
FUNDAMENTUS_PRE_PROCESSED_DFP = Variable.get('FUNDAMENTUS_PRE_PROCESSED_DFP')
FUNDAMENTUS_ANALYTICAL = Variable.get('FUNDAMENTUS_ANALYTICAL')

with DAG(
    dag_id='financial_information',
    start_date=datetime(2022, 12, 16),
    schedule_interval='10 19 * * 1-5',
    catchup=False
) as dag:

    environment = PythonOperator(
        task_id='path_environment',
        python_callable=path_environment
    )

    pp_union_fin_dfp = SparkSubmitOperator(
        task_id=f'pp_union_financial_dfp',
        conn_id='spark',
        application='/opt/sparkFiles/financial_information_analytical.py',
        name='pp_union_financial_',
        application_args=[
        '--type_file', 'dfp_all']
    )

    upload_s3_union_fin_dfp = PythonOperator(
        task_id='upload_s3_union_financial_dfp',
        python_callable=load_bucket,
        op_kwargs={
            'bucket':f'{FUNDAMENTUS_ANALYTICAL}',
            'dataType':'pre-processed-stock',
            'execution_date':EXECUTION_DATE,
            'delete':True,
        }
    )

    pp_union_fin_itr = SparkSubmitOperator(
        task_id=f'pp_union_financial_itr',
        conn_id='spark',
        application='/opt/sparkFiles/financial_information_analytical.py',
        name='pp_union_financial_',
        application_args=[
        '--type_file', 'itr_all']
    )

    upload_s3_union_fin_itr = PythonOperator(
        task_id='upload_s3_union_financial_itr',
        python_callable=load_bucket,
        op_kwargs={
            'bucket':f'{FUNDAMENTUS_ANALYTICAL}',
            'dataType':'pre-processed-stock',
            'execution_date':EXECUTION_DATE,
            'delete':True,
        }
    )

environment >> pp_union_fin_dfp >> upload_s3_union_fin_dfp >> pp_union_fin_itr >> upload_s3_union_fin_itr