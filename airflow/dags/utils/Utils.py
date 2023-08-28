


def download_bucket_s3(s3, bucket, path):
    
    import os
    import boto3

    def _get_objects(s3, response, bucket, path):
        for i in response['Contents']:
            source = i['Key']
            destination = os.path.join(path, source)
            if not os.path.exists(os.path.dirname(destination)):
                os.makedirs(os.path.dirname(destination))
            s3.download_file(Bucket=bucket,Key=source, Filename=destination)

    
    response = s3.list_objects_v2(Bucket=bucket)
    _get_objects(s3=s3,response=response, bucket=bucket, path=path)
    while response.get('IsTruncated'):
        TOKEN = response.get('NextContinuationToken')
        response = s3.list_objects_v2(Bucket=bucket, ContinuationToken=TOKEN)
        _get_objects(s3=s3, response=response, bucket=bucket, path=path)


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
    if 'raw-fca' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'raw-fca'))
    if 'raw-itr' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'raw-itr'))
    if 'raw-dfp' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'raw-dfp'))
    if 'pre-processed-fca-general-register' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'pre-processed-fca-general-register'))
    if 'pre-processed-fca-stock-type' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'pre-processed-fca-stock-type'))
    if 'pre-processed-dfp' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'pre-processed-dfp'))
    if 'pre-processed-itr' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'pre-processed-itr'))
    if 'pre-processed-stock' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'pre-processed-stock'))
    if 'analytical' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'analytical'))
    if 'monitoring' not in list_folders:
        os.mkdir(os.path.join(PATH_DATALAKE, 'monitoring'))

    DIR_PATH_RAW_STOCK = os.path.join(PATH_DATALAKE, 'raw-stock')
    DIR_PATH_RAW_FCA = os.path.join(PATH_DATALAKE, 'raw-fca')
    DIR_PATH_RAW_ITR = os.path.join(PATH_DATALAKE, 'raw-itr')
    DIR_PATH_RAW_DFP = os.path.join(PATH_DATALAKE, 'raw-dfp')

    DIR_PATH_PROCESSED_FCA_GENERAL_REGISTER = os.path.join(PATH_DATALAKE, 'pre-processed-fca-general-register')
    DIR_PATH_PROCESSED_FCA_STOCK_TYPE = os.path.join(PATH_DATALAKE, 'pre-processed-fca-stock-type')
    DIR_PATH_PROCESSED_DFP = os.path.join(PATH_DATALAKE, 'pre-processed-dfp')
    DIR_PATH_PROCESSED_ITR = os.path.join(PATH_DATALAKE, 'pre-processed-itr')
    DIR_PATH_PROCESSED_STOCK = os.path.join(PATH_DATALAKE, 'pre-processed-stock')

    DIR_PATH_ANALYTICAL = os.path.join(PATH_DATALAKE, 'analytical')
    DIR_PATH_MONITORING = os.path.join(PATH_DATALAKE, 'monitoring')

    ti.xcom_push(key='DIR_PATH', value=DIR_PATH)
    # Raw folders
    ti.xcom_push(key='DIR_PATH_RAW_STOCK', value=DIR_PATH_RAW_STOCK)
    ti.xcom_push(key='DIR_PATH_RAW_FCA', value=DIR_PATH_RAW_FCA)
    ti.xcom_push(key='DIR_PATH_RAW_ITR', value=DIR_PATH_RAW_ITR)
    ti.xcom_push(key='DIR_PATH_RAW_DFP', value=DIR_PATH_RAW_DFP)
    # Pre-processed folders
    ti.xcom_push(key='DIR_PATH_PROCESSED_FCA_GENERAL_REGISTER', value=DIR_PATH_PROCESSED_FCA_GENERAL_REGISTER)
    ti.xcom_push(key='DIR_PATH_PROCESSED_FCA_STOCK_TYPE', value=DIR_PATH_PROCESSED_FCA_STOCK_TYPE)
    ti.xcom_push(key='DIR_PATH_PROCESSED_DFP', value=DIR_PATH_PROCESSED_DFP)
    ti.xcom_push(key='DIR_PATH_PROCESSED_ITR', value=DIR_PATH_PROCESSED_ITR)
    ti.xcom_push(key='DIR_PATH_PROCESSED_STOCK', value=DIR_PATH_PROCESSED_STOCK)
    # Analytical
    ti.xcom_push(key='DIR_PATH_ANALYTICAL', value=DIR_PATH_ANALYTICAL)  
    # Monitoring
    ti.xcom_push(key='DIR_PATH_MONITORING', value=DIR_PATH_MONITORING)  


def unzippded_files(ti, dataType):

    import os
    import re
    import zipfile

    if dataType == 'fca':
        DIR_PATH_RAW = ti.xcom_pull(key='DIR_PATH_RAW_FCA', task_ids='path_environment')
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
        print('Unziping:{file}')
        try:
            with zipfile.ZipFile(os.path.join(DIR_PATH_RAW, file), 'r') as zip_ref:
                zip_ref.extractall(DIR_PATH_RAW)
        except:
            print('Error unzip')


def delete_objects(bucket, prefix):
    
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook('s3_conn')
    hook.list_keys(bucket_name=bucket, prefix=prefix)


def load_bucket(ti, bucket, dataType, execution_date, delete=None):

    import os
    import re
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook


    def __lag_execution_date(execution_date):

        from datetime import datetime, timedelta
        date_reference = datetime.strptime(execution_date, "%Y-%m-%d").date()
        date_reference = date_reference + timedelta(-1)
        date_reference = date_reference.strftime('%Y-%m-%d')
        date_reference = date_reference.replace('-', '_')

        return date_reference


    def __load_raw_dfp_itr_fca(DIR_PATH, dataType):

        files_foder = [file for file in os.listdir(DIR_PATH) if (
            file.endswith('.zip')) and re.findall(dataType, file)]
        for file in files_foder:
            hook.load_file(filename=os.path.join(
                DIR_PATH, f'{file}'), bucket_name=bucket, key=f'{file}', replace=True)


    def __load_pp(DIR_PATH, dataType, delete):

        folder_list = [file for file in os.listdir(DIR_PATH) if re.findall(dataType, file)]
        print(folder_list)

        for folder in folder_list:
            if delete:
                delete_folder = hook.list_keys(bucket_name=bucket, prefix=folder)
                hook.delete_objects(bucket=bucket, keys=delete_folder)
            try:
                DIR_PATH_FILE = os.path.join(DIR_PATH, folder)
                files_list = [file for file in os.listdir(DIR_PATH_FILE)]
                for file in files_list:
                    hook.load_file(filename=f'{DIR_PATH_FILE}/{file}', bucket_name=bucket, key=f'{folder}/{file}', replace=True)
            except:
                print('Is not a folder!')


    def __load_pp_partition(DIR_PATH, dataType):

        isthere = hook.list_keys(bucket_name=bucket, prefix=dataType)
        DIR_PATH = os.path.join(DIR_PATH, dataType)
        partition_folders = [file for file in os.listdir(DIR_PATH)]

        print('Inicio do Loop')
        print(partition_folders)
        for folder in partition_folders:
            print(folder)
            if folder not in isthere:
                DIR_PATH_PARTITION = os.path.join(DIR_PATH, folder)
                if os.path.isdir(os.path.join(DIR_PATH, folder)):
                    print(DIR_PATH_PARTITION)
                    partition_files = [file for file in os.listdir(DIR_PATH_PARTITION)]
                    print(partition_files)
                    for file in partition_files:
                        hook.load_file(filename=f'{DIR_PATH_PARTITION}/{file}', bucket_name=bucket, key=f'pp_stock_union.parquet/{folder}/{file}', replace=True)
                else:
                    hook.load_file(filename=f'{DIR_PATH_PARTITION}', bucket_name=bucket, key=f'pp_stock_union.parquet/{folder}', replace=True)
    # s3_conn
    hook = S3Hook('aws_default')
    extract_at = execution_date.replace('-', '_')
    print(extract_at)

    if dataType == 'raw-fca':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_RAW_FCA', task_ids='path_environment')
        dataType = f'extracted_{extract_at}_fca_cia_aberta'
        __load_raw_dfp_itr_fca(DIR_PATH=DIR_PATH, dataType=dataType)

    elif dataType == 'raw-dfp':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_RAW_DFP', task_ids='path_environment')
        dataType = f'extracted_{extract_at}_dfp_cia_aberta'
        __load_raw_dfp_itr_fca(DIR_PATH=DIR_PATH, dataType=dataType)

    elif dataType == 'raw-itr':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_RAW_ITR', task_ids='path_environment')
        dataType = f'extracted_{extract_at}_itr_cia_aberta'
        __load_raw_dfp_itr_fca(DIR_PATH=DIR_PATH, dataType=dataType)

    elif dataType == 'raw-stock':
        date_reference = __lag_execution_date(execution_date=execution_date)    
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_RAW_STOCK', task_ids='path_environment')
        dataType = f'extracted_{date_reference}_stock.parquet'
        __load_pp(DIR_PATH=DIR_PATH, dataType=dataType, delete=delete)

    elif dataType == 'pre-processed-fca-general-register':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_PROCESSED_FCA_GENERAL_REGISTER', task_ids='path_environment')
        dataType = 'fca_aberta_geral'
        __load_pp(DIR_PATH=DIR_PATH, dataType=dataType, delete=delete)

    elif dataType == 'pre-processed-fca-stock-type':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_PROCESSED_FCA_STOCK_TYPE', task_ids='path_environment')
        dataType = 'fca_valor_mobiliario'
        __load_pp(DIR_PATH=DIR_PATH, dataType=dataType, delete=delete)


    elif dataType == 'pre-processed-dfp':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_PROCESSED_DFP', task_ids='path_environment')
        dataType = 'dfp'
        __load_pp(DIR_PATH=DIR_PATH, dataType=dataType, delete=delete)

    elif dataType == 'pre-processed-itr':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_PROCESSED_ITR', task_ids='path_environment')
        dataType = 'itr'
        __load_pp(DIR_PATH=DIR_PATH, dataType=dataType, delete=delete)

    elif dataType == 'pre-processed-stock':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_PROCESSED_STOCK', task_ids='path_environment')
        dataType = f'pp_stock_union.parquet'
        __load_pp(DIR_PATH=DIR_PATH, dataType=dataType, delete=delete)
    
    elif dataType == 'analytical-financial-information-dfp':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_ANALYTICAL', task_ids='path_environment')
        dataType = f'analytical_FPD_financial_information.parquet'
        __load_pp(DIR_PATH=DIR_PATH, dataType=dataType, delete=delete)

    elif dataType == 'analytical-financial-information-itr':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_ANALYTICAL', task_ids='path_environment')
        dataType = f'analytical_ITR_financial_information.parquet'
        __load_pp(DIR_PATH=DIR_PATH, dataType=dataType, delete=delete)

    elif dataType == 'analytical-financial-dfc-mi-fpd':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_ANALYTICAL', task_ids='path_environment')
        dataType = f'analytical_dfp_DFC_MI.parquet'
        __load_pp(DIR_PATH=DIR_PATH, dataType=dataType, delete=delete)

    elif dataType == 'monitoring-dre':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_MONITORING', task_ids='path_environment')
        dataType = 'monitoring'
        __load_pp(DIR_PATH=DIR_PATH, dataType=dataType, delete=delete)


    def download_s3(ti, bucket_name, key, dataType):

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook('s3_conn')

        if dataType == 'pre-processed-stock':
            DIR_PATH = ti.xcom_pull(key='DIR_PATH_PROCESSED_STOCK', task_ids='path_environment')
        
        try:
            hook.download_file(key=key, bucket_name=bucket_name, local_path=DIR_PATH)
        except:
            print('There is no file.')


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