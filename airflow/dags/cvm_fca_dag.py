from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from groups.group_extractions_cvm import extraction_cvm
from groups.group_pre_processing_cvm import pre_processing_cvm
from utils.Utils import path_environment, load_bucket, unzippded_files
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator


EXECUTION_DATE = '{{ ds }}'
FUNDAMENTUS_RAW_FCA = Variable.get('FUNDAMENTUS_RAW_FCA')
FUNDAMENTUS_PRE_PROCESSED_FCA_GENERAL_REGISTER = Variable.get('FUNDAMENTUS_PRE_PROCESSED_FCA_GENERAL_REGISTER')
FUNDAMENTUS_PRE_PROCESSED_FCA_STOCK_TYPE = Variable.get('FUNDAMENTUS_PRE_PROCESSED_FCA_STOCK_TYPE')

with DAG(
    dag_id='cvm_fca',
    start_date=datetime(2022, 10, 14),
    schedule_interval='0 18 * * 5',
    catchup=False
) as dag:

    environment = PythonOperator(
        task_id='path_environment',
        python_callable=path_environment
    )

    ext_cvm_fca = extraction_cvm(dataType='raw-fca', execution_date=EXECUTION_DATE)

    upload_s3_r = PythonOperator(
        task_id='upload_s3_raw_fca',
        python_callable=load_bucket,
        op_kwargs={
            'bucket': f'{FUNDAMENTUS_RAW_FCA}',
            'dataType': 'raw-fca',
            'execution_date': EXECUTION_DATE,
        }
    )

    unzip_cvm = PythonOperator(
        task_id='unzip_raw',
        python_callable=unzippded_files,
        op_kwargs={
            'dataType': 'fca',
        }
    )

    pp_cvm_fca_aberta_geral = pre_processing_cvm(dataType='fca_aberta_geral', execution_date=EXECUTION_DATE)

    upload_s3_pp_register_id = PythonOperator(
        task_id='upload_s3_pp_register',
        python_callable=load_bucket,
        op_kwargs={
            'bucket': f'{FUNDAMENTUS_PRE_PROCESSED_FCA_GENERAL_REGISTER}',
            'dataType': 'pre-processed-fca-general-register',
            'execution_date': EXECUTION_DATE,
            'delete':True,
        }
    )

    pp_cvm_fca_valor_mobiliario = pre_processing_cvm(dataType='fca_valor_mobiliario', execution_date=EXECUTION_DATE)
    
    upload_s3_pp_stock_type_id = PythonOperator(
        task_id='upload_s3_pp_stock_type',
        python_callable=load_bucket,
        op_kwargs={
            'bucket': f'{FUNDAMENTUS_PRE_PROCESSED_FCA_STOCK_TYPE}',
            'dataType': 'pre-processed-fca-stock-type',
            'execution_date': EXECUTION_DATE,
            'delete':True,
        }
    )

environment >> ext_cvm_fca >> upload_s3_r >> unzip_cvm >> pp_cvm_fca_aberta_geral >> upload_s3_pp_register_id >> pp_cvm_fca_valor_mobiliario >> upload_s3_pp_stock_type_id