import os
import re
import utils.documents as dc
from datetime import date
import urllib.request
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# Environment
years_list = dc.years_list

def _extraction_cvm(ti, dataType:str, year:int, execution_date:str):
    
    from utils.documents import repository_registration, repository_DFP, repository_ITR

    # Local Path
    DIR_PATH_RAW = ti.xcom_pull(key='DIR_PATH_RAW', task_ids='path_environment')
    DIR_PATH_RAW_DFP = ti.xcom_pull(key='DIR_PATH_RAW_DFP', task_ids='path_environment')
    DIR_PATH_RAW_ITR = ti.xcom_pull(key='DIR_PATH_RAW_ITR', task_ids='path_environment')
    extract_at = execution_date.replace('-', '_')
    
    if 'registration' in dataType:
            urllib.request.urlretrieve(repository_registration, os.path.join(DIR_PATH_RAW, 'extracted_{extract_at}_cad_cia_aberta.csv'))
    # Year (yearly)
    if 'dfp' in dataType:
        urllib.request.urlretrieve(repository_DFP+f'dfp_cia_aberta_{year}.zip', os.path.join(DIR_PATH_RAW_DFP, f'extracted_{extract_at}_dfp_cia_aberta_{year}.zip'))
    # Quarter (quarterly) 
    if 'itr' in dataType:
        try:
           urllib.request.urlretrieve(repository_ITR+f'itr_cia_aberta_{year}.zip', os.path.join(DIR_PATH_RAW_ITR, f'extracted_{extract_at}_itr_cia_aberta_{year}.zip'))
        except:
            print(f'The file itr_cia_aberta_{year}.zip is not avaliable.')


def extraction_cvm(dataType, execution_date):

    with TaskGroup('extraction_cvm', tooltip='extraction cvm') as group:

        for year in years_list:
            ex_itr_group = PythonOperator(
                task_id=f'ext_raw_{dataType}_{year}',
                python_callable=_extraction_cvm,
                op_kwargs = {'dataType':dataType, 'year':year, 'execution_date':execution_date}
            )

        return group




