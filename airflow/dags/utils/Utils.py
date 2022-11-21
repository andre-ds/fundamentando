

def get_stock_symbols(restrictions):

    import investpy

    dataset_stocks = investpy.get_stocks(country='brazil')
    stocks_list = dataset_stocks['symbol'].to_list()
    stocks_list_on_pn = []
    if restrictions is True:
        for i in stocks_list:
            if '3' in i or '4' in i:
                stocks_list_on_pn.append(f'{i}.SA')
    else:
        stocks_list_on_pn.append(f'{i}.SA')

    return stocks_list_on_pn


def path_environment(ti):

    import os
    DIR_PATH = os.path.dirname(os.path.realpath('__file__'))
    list_folders = os.listdir(DIR_PATH)
    if 'datalake' not in list_folders:
        os.mkdir(os.path.join(DIR_PATH, 'datalake'))

    PATH_DATALAKE = os.path.join(DIR_PATH, 'datalake')
    # Creating temp folders
    list_folders = os.listdir(PATH_DATALAKE)
    if 'raw-stock' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'raw-stock'))
    if 'raw-registration' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'raw-registration'))
    if 'raw-itr' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'raw-itr'))
    if 'raw-dfp' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'raw-dfp'))
    if 'pre-processed-dfp' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'pre-processed-dfp'))
    if 'pre-processed-itr' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'pre-processed-itr'))
    if 'analytical' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'analytical'))

    DIR_PATH_RAW_REGISTRATION = os.path.join(PATH_DATALAKE, 'raw-registration')
    DIR_PATH_RAW_STOCK = os.path.join(PATH_DATALAKE, 'raw-stock')
    DIR_PATH_RAW_ITR = os.path.join(PATH_DATALAKE, 'raw-itr')
    DIR_PATH_RAW_DFP = os.path.join(PATH_DATALAKE, 'raw-dfp')

    DIR_PATH_PROCESSED_DFP = os.path.join(PATH_DATALAKE, 'pre-processed-dfp')
    DIR_PATH_PROCESSED_ITR = os.path.join(PATH_DATALAKE, 'pre-processed-itr')

    ti.xcom_push(key='DIR_PATH', value=DIR_PATH)
    # Raw folders
    ti.xcom_push(key='DIR_PATH_RAW_STOCK', value=DIR_PATH_RAW_STOCK)
    ti.xcom_push(key='DIR_PATH_RAW_REGISTRATION', value=DIR_PATH_RAW_REGISTRATION)
    ti.xcom_push(key='DIR_PATH_RAW_ITR', value=DIR_PATH_RAW_ITR)
    ti.xcom_push(key='DIR_PATH_RAW_DFP', value=DIR_PATH_RAW_DFP)
    # Pre-processed folders
    ti.xcom_push(key='DIR_PATH_PROCESSED_DFP', value=DIR_PATH_PROCESSED_DFP)
    ti.xcom_push(key='DIR_PATH_PROCESSED_ITR', value=DIR_PATH_PROCESSED_ITR)


def unzippded_files(ti, dataType):

    import os
    import re
    import zipfile

    if dataType == 'registration':
        DIR_PATH_RAW = ti.xcom_pull(key='DIR_PATH_RAW_REGISTRATION', task_ids='path_environment')
        print(DIR_PATH_RAW)
    elif dataType == 'dfp':
        DIR_PATH_RAW = ti.xcom_pull(key='DIR_PATH_RAW_DFP', task_ids='path_environment')
        print(DIR_PATH_RAW)
    elif dataType == 'itr':
        DIR_PATH_RAW = ti.xcom_pull(key='DIR_PATH_RAW_ITR', task_ids='path_environment')

    list_files = [file.lower() for file in os.listdir(DIR_PATH_RAW) if (
        file.endswith('.zip')) and len(re.findall(dataType, file)) > 0]

    print(list_files)
    for file in list_files:
        try:
            with zipfile.ZipFile(os.path.join(DIR_PATH_RAW, file), 'r') as zip_ref:
                zip_ref.extractall(DIR_PATH_RAW)
        except:
            print('Error unzip')
            
def load_bucket(ti, bucket, dataType, execution_date):

    import os
    import re
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook


    def __load_raw_registration(DIR_PATH, dataType):

        hook.load_file(filename=os.path.join(DIR_PATH, dataType), bucket_name=bucket, key=dataType, replace=True)


    def __load_raw_dfp_itr(DIR_PATH, dataType):

        files_foder = [file for file in os.listdir(DIR_PATH) if (
            file.endswith('.zip')) and re.findall(dataType, file)]
        for file in files_foder:
            hook.load_file(filename=os.path.join(
                DIR_PATH, f'{file}'), bucket_name=bucket, key=f'{file}', replace=True)


    def __load_pp_cvm_dfp_itr(DIR_PATH, dataType):

        folder_list = [file for file in os.listdir(DIR_PATH) if re.findall(dataType, file)]

        for folder in folder_list:
            try:
                DIR_PATH_FILE = os.path.join(DIR_PATH, folder)
                files_list = [file for file in os.listdir(DIR_PATH_FILE)]
                for file in files_list:
                    hook.load_file(filename=f'{DIR_PATH_FILE}/{file}', bucket_name=bucket, key=f'{folder}/{file}', replace=True)
            except:
                print('Is not a folder!')



    hook = S3Hook('s3_conn')
    extract_at = execution_date.replace('-', '_')
    print(extract_at)

    if dataType == 'registration':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_RAW_REGISTRATION', task_ids='path_environment')
        dataType = f'extracted_{extract_at}_cad_cia_aberta.csv'
        __load_raw_registration(DIR_PATH=DIR_PATH, dataType=dataType)


    elif dataType == 'raw-dfp':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_RAW_DFP', task_ids='path_environment')
        dataType = f'extracted_{extract_at}_dfp_cia_aberta'
        __load_raw_dfp_itr(DIR_PATH=DIR_PATH, dataType=dataType)

    elif dataType == 'raw-itr':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_RAW_ITR', task_ids='path_environment')
        dataType = f'extracted_{extract_at}_itr_cia_aberta'
        __load_raw_dfp_itr(DIR_PATH=DIR_PATH, dataType=dataType)

    elif dataType == 'raw-stock':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_RAW', task_ids='path_environment')

        dataType = f'extracted_{extract_at}_stock.parquet'
        folder_list = [file for file in os.listdir(DIR_PATH) if re.findall(dataType, file)]
        for folder in folder_list:
            try:
                DIR_PATH_FILE = os.path.join(DIR_PATH, folder)
                files_list = [file for file in os.listdir(DIR_PATH_FILE)]
                for file in files_list:
                    hook.load_file(filename=f'{DIR_PATH_FILE}/{file}', bucket_name=bucket, key=f'{folder}/{file}', replace=True)
            except:
                print('Is not a folder!')

    elif dataType == 'pre-processed-dfp':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_PROCESSED_DFP', task_ids='path_environment')
        dataType = 'dfp'
        __load_pp_cvm_dfp_itr(DIR_PATH=DIR_PATH, dataType=dataType)

    elif dataType == 'pre-processed-itr':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_PROCESSED_ITR', task_ids='path_environment')
        dataType = 'itr'
        __load_pp_cvm_dfp_itr(DIR_PATH=DIR_PATH, dataType=dataType)

'''
    def __load_pp_cvm_dfp_itr(DIR_PATH, dataType):

        folder_list = [file for file in os.listdir(DIR_PATH) if re.findall(dataType, file)]
        
        for folder in folder_list:
            files_foder = [file for file in os.listdir(os.path.join(DIR_PATH, folder))]
            for partition in files_foder:
                DIR_PATH_PARTITION = os.path.join(os.path.join(DIR_PATH, folder), partition)
                if os.path.isdir(DIR_PATH_PARTITION):
                    partition_files = [file for file in os.listdir(DIR_PATH_PARTITION)]
                    for file in partition_files:
                        hook.load_file(filename=f'{DIR_PATH_PARTITION}/{file}', bucket_name=bucket, key=f'{folder}/{partition}/{file}', replace=True)
'''