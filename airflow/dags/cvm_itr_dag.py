import os
from datetime import datetime
from airflow import DAG
from groups.group_extractions_cvm import extraction_cvm
from groups.group_pre_processing_cvm import pre_processing_cvm
from utils.Utils import path_environment, unzippded_files, load_bucket
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator


EXECUTION_DATE = '{{ ds }}'

with DAG(
    dag_id='cvm_itr',
    start_date=datetime(2022, 9, 29),
    schedule_interval='0 18 * * 5',
    catchup=False
) as dag:

    bash_test = BashOperator(
        task_id='teste_id',
        bash_command=f'echo {EXECUTION_DATE}'
    )

    environment = PythonOperator(
        task_id='path_environment',
        python_callable=path_environment
    )

    ext_cvm_itr = extraction_cvm(dataType='itr')

    upload_s3_r = PythonOperator(
        task_id='upload_s3_raw_itr',
        python_callable=load_bucket,
        op_kwargs={
            'bucket': 'fundamentus-raw',
            'dataType': 'raw-itr',
            'execution_date': EXECUTION_DATE
        }
    )

    unzip_cvm = PythonOperator(
        task_id='unzip_raw_itr',
        python_callable=unzippded_files,
        op_kwargs={
            'dataType': 'itr',
        }
    )

    pp_cvm_itr_dre = pre_processing_cvm(dataType='itr_dre')
    pp_cvm_itr_bpp = pre_processing_cvm(dataType='itr_bpp')
    pp_cvm_itr_bpa = pre_processing_cvm(dataType='itr_bpa')

    upload_s3_p = PythonOperator(
        task_id='upload_s3_pp_itr',
        python_callable=load_bucket,
        op_kwargs={
            'bucket': 'fundamentus-pre-processed',
            'dataType': 'pre-processed-itr',
            'execution_date': EXECUTION_DATE
        }
    )

bash_test >> environment >> ext_cvm_itr >> upload_s3_r >> unzip_cvm >> [
    pp_cvm_itr_dre, pp_cvm_itr_bpa, pp_cvm_itr_bpp] >> upload_s3_p
