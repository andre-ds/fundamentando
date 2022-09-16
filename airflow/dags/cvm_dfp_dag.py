import os
from datetime import datetime
from airflow import DAG
from groups.group_extractions_cvm import extraction_cvm
from groups.group_pre_processing_cvm import pre_processing_cvm
from utils.Utils import path_environment, unzippded_files, load_bucket
from airflow.operators.python import PythonOperator


with DAG(
    dag_id='cvm_dfp',
    start_date=datetime(2022, 8, 9),
    schedule_interval='0 12 * * 5',
    catchup=False
) as dag:

    environment = PythonOperator(
        task_id='path_environment',
        python_callable=path_environment
    )

    ext_cvm_dfp = extraction_cvm(dataType='dfp')

    upload_s3_r = PythonOperator(
        task_id='upload_s3_raw_dfp',
        python_callable=load_bucket,
        op_kwargs={
            'path':'raw',
            'bucket':'fundamentus-raw',
            'dataType':'dfp'
        }
    )

    unzip_cvm = PythonOperator(
        task_id='unzip_raw_dfp',
        python_callable=unzippded_files,
        op_kwargs={
            'dataType': 'dfp',
        }
    )
    pp_cvm_dfp_dre = pre_processing_cvm(dataType='dfp_dre')
    pp_cvm_dfp_bpa = pre_processing_cvm(dataType='dfp_bpa')
    pp_cvm_dfp_bpp = pre_processing_cvm(dataType='dfp_bpp')

    upload_s3_p = PythonOperator(
        task_id='upload_s3_pp_dfp',
        python_callable=load_bucket,
        op_kwargs={
            'path':'pre-processed',
            'bucket':'fundamentus-pre-processed',
            'dataType':'dfp'
        }
    )


environment >> ext_cvm_dfp >> upload_s3_r >> unzip_cvm >> [pp_cvm_dfp_dre, pp_cvm_dfp_bpa, pp_cvm_dfp_bpp] >> upload_s3_p

