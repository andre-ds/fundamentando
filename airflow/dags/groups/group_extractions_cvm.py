import os
import re
import utils.documents as dc
from datetime import date
import urllib.request
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# Environment
#years_list = [*range(2011, date.today().year+1, 1)]
#years_list = [2020, 2021]
years_list = dc.years_list

def _extraction_cvm(ti, dataType:str, year:int):
    
    # Date
    todaystr = re.sub('-', '_', str((date.today())))
    # Local Path
    DIR_PATH_RAW = ti.xcom_pull(key='DIR_PATH_RAW', task_ids='path_environment')
    # CVM-Repository
    repository_registration = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv'
    repository_DFP = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'
    repository_ITR = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/'

    if 'registration' in dataType:
            urllib.request.urlretrieve(repository_registration, os.path.join(DIR_PATH_RAW, 'extracted_{todaystr}_cad_cia_aberta.csv'))
    # Year (yearly)
    if 'dfp' in dataType:
        urllib.request.urlretrieve(repository_DFP+f'dfp_cia_aberta_{year}.zip', os.path.join(DIR_PATH_RAW, f'extracted_{todaystr}_dfp_cia_aberta_{year}.zip'))
    # Quarter (quarterly) 
    if 'itr' in dataType:
        try:
           urllib.request.urlretrieve(repository_ITR+f'itr_cia_aberta_{year}.zip', os.path.join(DIR_PATH_RAW, f'extracted_{todaystr}_itr_cia_aberta_{year}.zip'))
        except:
            print(f'The file itr_cia_aberta_{year}.zip is not avaliable.')


def extraction_cvm_itr():

    with TaskGroup('extraction_cvm_itr', tooltip='extraction cvm itr') as group:

        for year in years_list:
            ex_itr_group = PythonOperator(
                task_id=f'ext_raw_itr_{year}',
                python_callable=_extraction_cvm,
                op_kwargs = {'dataType':'itr', 'year':year}
            )

        return group

def extraction_cvm_dfp():

    with TaskGroup('extraction_cvm_dfp', tooltip='extraction cvm dfp') as group:

        for year in years_list:
            ex_dfp_group = PythonOperator(
                task_id=f'ext_raw_dfp_{year}',
                python_callable=_extraction_cvm,
                op_kwargs = {'dataType':'dfp', 'year':year}
            )

        
        return group



