import os
from utils.documents import years_list
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash_operator import BashOperator


def pp_cvm_dfp_dre():

    
    with TaskGroup('pre_processing_cvm_dfp_dre', tooltip='Pre-processing cvm DRE dfp') as group:

        for year in years_list:
            FILE_PATH = 'spark-submit /opt/sparkFiles/pre_processing_cvm.py --dataType "dfp_dre" --years_list {year}'
            pp_dfp_dre_group = BashOperator(
                task_id=f'pp_dfp_dre_id_{year}',
                bash_command=FILE_PATH.format(year=year)
            )

        return group


def pp_cvm_itr_dre():

    
    with TaskGroup('pre_processing_cvm_itr_dre', tooltip='Pre-processing cvm DRE itr') as group:

        for year in years_list:
            FILE_PATH = 'spark-submit /opt/sparkFiles/pre_processing_cvm.py --dataType "itr_dre" --years_list {year}'
            pp_itr_dre_group = BashOperator(
                task_id=f'pp_itr_dre_id_{year}',
                bash_command=FILE_PATH.format(year=year)
            )

        return group


def pp_cvm_itr_bpp():

    
    with TaskGroup('pre_processing_cvm_itr_bpp', tooltip='Pre-processing cvm BPP itr') as group:

        for year in years_list:
            FILE_PATH = 'spark-submit /opt/sparkFiles/pre_processing_cvm.py --dataType "itr_bpp" --years_list {year}'
            pp_itr_dre_group = BashOperator(
                task_id=f'pp_itr_bpp_id_{year}',
                bash_command=FILE_PATH.format(year=year)
            )

        return group


def pp_cvm_itr_bpa():

    
    with TaskGroup('pre_processing_cvm_itr_bpa', tooltip='Pre-processing cvm BPA itr') as group:

        for year in years_list:
            FILE_PATH = 'spark-submit /opt/sparkFiles/pre_processing_cvm.py --dataType "itr_bpa" --years_list {year}'
            pp_itr_dre_group = BashOperator(
                task_id=f'pp_itr_bpa_id_{year}',
                bash_command=FILE_PATH.format(year=year)
            )

        return group