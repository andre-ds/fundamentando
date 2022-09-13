import os
from utils.documents import years_list
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash_operator import BashOperator


def pre_processing_cvm_dfp_dre():

    
    with TaskGroup('pre_processing_cvm_dfp', tooltip='pre processing cvm dfp') as group:

        for year in years_list:
            FILE_PATH = 'spark-submit /opt/sparkFiles/pre_processing_cvm.py --dataType "dfp_dre" --years_list {year}'
            pp_dfp_dre_group = BashOperator(
                task_id=f'pp_dfp_dre_id_{year}',
                bash_command=FILE_PATH.format(year=year)
            )

        return group



