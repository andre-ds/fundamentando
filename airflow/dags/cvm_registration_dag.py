from datetime import datetime
from airflow import DAG
from groups.group_extractions_cvm import extraction_cvm
from groups.group_pre_processing_cvm import pre_processing_cvm
from utils.Utils import path_environment, load_bucket
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator


EXECUTION_DATE = '{{ ds }}'


def _extraction_cvm(ti, execution_date:str):
    
    import os
    import urllib.request
    from utils.documents import repository_registration

    # Local Path
    DIR_PATH_RAW_REGISTRATION = ti.xcom_pull(key='DIR_PATH_RAW_REGISTRATION', task_ids='path_environment')
    extract_at = execution_date.replace('-', '_')   
    urllib.request.urlretrieve(repository_registration, os.path.join(DIR_PATH_RAW_REGISTRATION, f'extracted_{extract_at}_cad_cia_aberta.csv'))


with DAG(
    dag_id='cvm_registration',
    start_date=datetime(2022, 11, 15),
    schedule_interval='0 12 1 * *',
    catchup=False
) as dag:


    environment = PythonOperator(
        task_id='path_environment',
        python_callable=path_environment
    )

    ext_cvm_registration = PythonOperator(
        task_id='ex_registration_id',
        python_callable=_extraction_cvm,
        op_kwargs={
            'dataType':'registration',
            'execution_date': EXECUTION_DATE
        }
    )

    upload_s3_r = PythonOperator(
        task_id='upload_s3_raw_registration',
        python_callable=load_bucket,
        op_kwargs={
            'bucket': 'fundamentus-raw-registration',
            'dataType': 'registration',
            'execution_date': EXECUTION_DATE
        }
    )

environment >> ext_cvm_registration >> upload_s3_r