import os
from utils.documents import years_list
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash_operator import BashOperator


def __taskgroup_ids(dataType):
    if dataType == 'dfp_dre':
        group_id = 'pre_processing_cvm_dfp_dre'
        tooltip  = 'pre processing cvm dfp dre'
    elif dataType == 'dfp_bpp':
        group_id = 'pre_processing_cvm_dfp_bpp'
        tooltip  = 'pre processing cvm dfp bpp'
    elif dataType == 'dfp_bpa':
        group_id = 'pre_processing_cvm_dfp_bpa'
        tooltip  = 'pre processing cvm dfp bpa'
    elif dataType == 'itr_dre':
        group_id = 'pre_processing_cvm_itr_dre'
        tooltip  = 'pre processing cvm itr dre'
    elif dataType == 'itr_bpp':
        group_id = 'pre_processing_cvm_itr_bpp'
        tooltip  = 'pre processing cvm itr bpp'
    elif dataType == 'itr_bpa':
        group_id = 'pre_processing_cvm_itr_bpa'
        tooltip  = 'pre processing cvm itr bpa'

    return group_id, tooltip


def pre_processing_cvm(dataType, execution_date):

    group_id, tooltip = __taskgroup_ids(dataType=dataType)

    with TaskGroup(group_id=group_id, tooltip=tooltip) as group:

        for year in years_list:
            FILE_PATH = 'spark-submit /opt/sparkFiles/pre_processing_cvm.py --dataType "{dataType}" --years_list {year} --execution_date {execution_date}'
            pre_processing_cvm = BashOperator(
                task_id=f'pre_processing_cvm_{dataType}_{year}',
                bash_command=FILE_PATH.format(dataType=dataType, year=year, execution_date=execution_date)
            )

        return group