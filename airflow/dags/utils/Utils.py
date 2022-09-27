

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


def unzippded_files(ti, dataType):

    import os
    import re
    import zipfile

    DIR_PATH_RAW = ti.xcom_pull(key='DIR_PATH_RAW', task_ids='path_environment')
    list_files = [file.lower() for file in os.listdir(DIR_PATH_RAW) if (file.endswith('.zip')) and re.findall(dataType, file)]
    for file in list_files:
        with zipfile.ZipFile(os.path.join(DIR_PATH_RAW, file), 'r') as zip_ref:
            zip_ref.extractall(DIR_PATH_RAW)


def load_bucket(ti, path, bucket, dataType, execution_date=None):
    
    import os
    import re
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook('s3_conn')
    if path == 'raw':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_RAW', task_ids='path_environment')
        files_foder = [file for file in os.listdir(DIR_PATH) if (file.endswith('.zip')) and re.findall(dataType, file)]
        
        for file in files_foder:
            hook.load_file(filename=os.path.join(DIR_PATH, f'{file}'), bucket_name=bucket, key=f'{file}', replace=True)
    
    elif path == 'raw-stock':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_RAW', task_ids='path_environment')
        
        if execution_date is not None:
            extract_at = execution_date.replace('-', '_')
            dataType = f'extracted_{extract_at}_stock.parquet'
        folder_list = [file for file in os.listdir(DIR_PATH) if re.findall(dataType, file)] 
        for folder in folder_list:
            try:
                DIR_PATH_FILE= os.path.join(DIR_PATH, folder)
                files_list = [file for file in os.listdir(DIR_PATH_FILE)]
                for file in files_list:
                    hook.load_file(filename=f'{DIR_PATH_FILE}/{file}', bucket_name=bucket, key=f'{folder}/{file}', replace=True)
            except:
                print('Is not a folder!')
    elif path == 'pre-processed':
        DIR_PATH = ti.xcom_pull(key='DIR_PATH_PROCESSED', task_ids='path_environment')
        folder_list = [file for file in os.listdir(DIR_PATH) if re.findall(dataType, file)]  
        for folder in folder_list:
            files_foder = [file for file in os.listdir(os.path.join(DIR_PATH, folder))]
            for partition in files_foder:
                try:
                    DIR_PATH_PARTITION = os.path.join(os.path.join(DIR_PATH, folder), partition)
                    partition_files = [file for file in os.listdir(DIR_PATH_PARTITION)]
                    for file in partition_files:
                        hook.load_file(filename=f'{DIR_PATH_PARTITION}/{file}', bucket_name=bucket, key=f'{folder}/{partition}/{file}', replace=True)
                except:
                    print('Is not a folder!')