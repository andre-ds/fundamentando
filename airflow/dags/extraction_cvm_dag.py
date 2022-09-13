import os
from distutils.log import error
from datetime import datetime, date, timedelta
from airflow import DAG
from groups.group_extractions_cvm import extraction_cvm_itr, extraction_cvm_dfp
from groups.group_pre_processing_cvm import pre_processing_cvm_dfp_dre
from utils.Utils import unzippded_files, load_bucket
import utils.documents as dc
from airflow.operators.python import PythonOperator


# Environment
DIR_PATH = os.path.dirname(os.path.realpath('__file__'))


def _path_environment(ti):
    
    import os
    DIR_PATH = os.path.dirname(os.path.realpath('__file__'))
    list_folders = os.listdir(DIR_PATH)
    if 'datalake' not in list_folders:
        os.mkdir(os.path.join(DIR_PATH, 'datalake'))
    
    PATH_DATALAKE = os.path.join(DIR_PATH, 'datalake')
    # Creating temp folders
    list_folders = os.listdir(PATH_DATALAKE)
    if 'raw' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'raw'))
    if 'pre-processed' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'pre-processed'))
    if 'analytical' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'analytical'))
        
    DIR_PATH_RAW = os.path.join(PATH_DATALAKE, 'raw')
    DIR_PATH_PROCESSED = os.path.join(PATH_DATALAKE, 'pre-processed')
    ti.xcom_push(key='DIR_PATH', value=DIR_PATH)
    ti.xcom_push(key='DIR_PATH_RAW', value=DIR_PATH_RAW)
    ti.xcom_push(key='DIR_PATH_PROCESSED', value=DIR_PATH_PROCESSED)


with DAG(
    dag_id='extraction_cvm',
    start_date=datetime(2022, 8, 9),
    schedule_interval='@daily',
    catchup=False
) as dag:

    environment = PythonOperator(
        task_id='path_environment',
        python_callable=_path_environment,
        op_kwargs={'path':DIR_PATH}
    )

    ext_cvm_dfp = extraction_cvm_dfp()
    #ext_cvm_itr = extraction_cvm_itr()

    upload_s3 = PythonOperator(
        task_id='upload_s3_raw',
        python_callable=load_bucket,
        op_kwargs={
            'bucket':'deepfi-raw',
            'DIR_PATH': DIR_PATH
        }
    )

    unzip_dfp = PythonOperator(
        task_id='unzip_raw_dfp',
        python_callable=unzippded_files,
        op_kwargs={
            'dataType': 'dfp',
        }
    )

    pp_cvm_dfp_dre = pre_processing_cvm_dfp_dre()
    

#environment >> [ext_cvm_dfp, ext_cvm_itr] >> upload_s3 >> unzip_dfp
environment >> [ext_cvm_dfp] >> upload_s3 >> unzip_dfp >> pp_cvm_dfp_dre
#environment >> [ext_cvm_dfp] >> unzip_dfp >> pp_cvm_dfp
