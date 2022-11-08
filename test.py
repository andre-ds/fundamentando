from sparkFiles.PreProcessing import PreProcessing
from sparkFiles.sparkDocuments import schema_dre
from msilib import schema
from operator import concat, index
import boto3
import os
import re
import pandas as pd
from datetime import date, timedelta
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from botocore.exceptions import ClientError
from boto3.exceptions import S3TransferFailedError
import investpy as inv
from datetime import datetime
from pyspark.sql.functions import col, quarter, to_date, month, year, when, to_date, asc, months_between, round, concat, lit, regexp_replace, sum, max, lag, abs, collect_list, row_number, slice
from pyspark.sql.types import StructField, StructType, DateType, DoubleType, StringType, IntegerType, FloatType, ArrayType
import yfinance as yf
import investpy
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
import numpy as np



DIR_PATH = '/home/andre/Dropbox/projects/fundamentalista_pipeline/datalake'
DIR_PATH_RAW = '/home/andre/Dropbox/projects/fundamentalista_pipeline/datalake/raw'
DIR_PATH_PROCESSED_ITR = '/home/andre/Dropbox/projects/fundamentalista_pipeline/datalake/pre-processed-itr'
DIR_PATH_PROCESSED_DFP = '/home/andre/Dropbox/projects/fundamentalista_pipeline/datalake/pre-processed-dfp'

'''
    bash_test = BashOperator(
        task_id='teste_id',
        bash_command=f'echo {EXECUTION_DATE}'
    )
'''
# S3 connection
s3 = boto3.client(
    service_name='s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

sk = SparkSession(SparkContext(conf=SparkConf())
                  .getOrCreate())


# Create an empty RDD with empty schema
schema_pp_dre = StructType([
    StructField('id_cvm', StringType(), True),
    StructField('id_cnpj', StringType(), True),
    StructField('txt_company_name', StringType(), True),
    StructField('dt_refer', DateType(), True),
    StructField('dt_fim_exerc', DateType(), True),
    StructField('dt_ini_exerc', DateType(), True),
    StructField('dt_year', IntegerType(), True),
    StructField('dt_quarter', IntegerType(), True),
    StructField('cost_goods_and_services', FloatType(), True),
    StructField('earnings_before_income_tax_and_social_contribution',
                FloatType(), True),
    StructField('earnings_before_interest_and_taxes', FloatType(), True),
    StructField('financial_results', FloatType(), True),
    StructField('groos_revenue', FloatType(), True),
    StructField('net_profit', FloatType(), True),
    StructField('operating_revenues_and_expenses', FloatType(), True),
    StructField('sales_revenue', FloatType(), True),
    StructField('processed_at', DateType(), True),
])

dataset = sk.createDataFrame(data=sk.sparkContext.emptyRDD(),
                             schema=schema_pp_dre)

# Append _union_quarters
pp = PreProcessing(spark_environment=sk)

years_list = ['2011','2012', '2013', '2014', '2015', '2016',
              '2017', '2018', '2019', '2020', '2021', '2022']
            
for year in years_list:
    dataset_itr = sk.read.parquet(os.path.join(
        DIR_PATH_PROCESSED_ITR, f'pp_itr_dre_{year}.parquet'))
    dataset_dfp = sk.read.parquet(os.path.join(
        DIR_PATH_PROCESSED_DFP, f'pp_dfp_dre_{year}.parquet'))
    df = pp._union_quarters(dataset_itr=dataset_itr, dataset_dfp=dataset_dfp)
    dataset = dataset.union(df)


# Calculating Lags
def get_lag(dataset, type, measure, variable, nlag, period):

    windowSpec  = Window.partitionBy('id_cvm').orderBy(['dt_year','dt_quarter'])
    dataset = dataset.withColumn(f'{type}_{measure}_{variable}_{nlag}{period}_lag',lag(variable, nlag).over(windowSpec))

    return dataset


def get_percentage_change(dataset, variable, nlag, period, period_text:None):

    windowSpec  = Window.partitionBy('id_cvm').orderBy(['dt_year','dt_quarter'])
   
    if period_text is not None:
        dataset = (
            dataset
            .withColumn('var_lag',lag(variable, nlag).over(windowSpec))
            .withColumn(f'pct_tx_{variable}_{period_text}', (col(variable)-col('var_lag'))/abs(col('var_lag')))
            .drop('var_lag')
        )       
    else:
        dataset = (
            dataset
            .withColumn('var_lag',lag(variable, nlag).over(windowSpec))
            .withColumn(f'pct_tx_{variable}_{nlag}{period}', (col(variable)-col('var_lag'))/abs(col('var_lag')))
            .drop('var_lag')
        )

    return dataset


def measure_run(dataset, variable, n_periods, measure_type):

    def mean_func(x):
        result = np.mean(x)
        return float(result)  

    def median_func(x):
        result = np.median(x)
        return float(result)   

    def std_func(x):
        result = np.std(x)
        return float(result)

    def var_func(x):
        
        result = np.var(x)
        return float(result)

    def min_func(x):
        
        result = np.min(x)
        return float(result)

    def max_func(x):
        
        result = np.max(x)
        return float(result)


    if measure_type == 'mean':
        variable_name = f'amt_avg_{variable}_{n_periods}'
        func_udf = udf(mean_func, DoubleType())
    elif measure_type == 'median':
        variable_name = f'amt_mda_{variable}_{n_periods}'
        func_udf = udf(median_func, DoubleType())
    elif measure_type == 'std':
        variable_name = f'amt_std_{variable}_{n_periods}'
        func_udf = udf(var_func, DoubleType())
    elif measure_type == 'var':
        variable_name = f'amt_var_{variable}_{n_periods}'
        func_udf = udf(std_func, DoubleType())
    elif measure_type == 'min':
        variable_name = f'amt_min_{variable}_{n_periods}'
        func_udf = udf(min_func, DoubleType())
    elif measure_type == 'max':
        variable_name = f'amt_max_{variable}_{n_periods}'
        func_udf = udf(max_func, DoubleType())

    windowSpec  = Window.partitionBy('id_cvm').orderBy(['dt_year','dt_quarter'])

    df = (
        dataset
        .orderBy(['id_cvm','dt_year','dt_quarter'])
        .groupBy('id_cvm')
        .agg(collect_list(variable).alias('array'))
    )

    dataset = (
        dataset
        .withColumn('row_n', row_number().over(windowSpec))
        .join(df, on='id_cvm', how='left')
        .withColumn('array_slice', slice(col('array'), col('row_n'), n_periods))
        .withColumn(variable_name, func_udf(col('array_slice')))
        .drop(*['array', 'array_slice'])
    )

    return dataset


variable_list = ['cost_goods_and_services', 'earnings_before_income_tax_and_social_contribution',
               'earnings_before_interest_and_taxes', 'financial_results', 'groos_revenue',
               'net_profit', 'operating_revenues_and_expenses', 'sales_revenue']

# Calculating Lag and percentage change
for v in variable_list:
    dataset = get_lag(dataset=dataset, type='amt', measure='tot', variable=v, nlag=1, period='m')
    dataset = get_lag(dataset=dataset, type='amt', measure='tot', variable=v, nlag=4, period='q', period_text='1y')
    dataset = get_percentage_change(dataset=dataset, variable=v, nlag=1, period='m')
    dataset = get_percentage_change(dataset=dataset, variable=v, nlag=4, period='q')

# Mean Beetween rows
for v in variable_list:
    print(f'Variable: {v}')
    dataset = measure_run(dataset=dataset, variable=v, n_periods=3, measure_type='mean')
for v in variable_list:
    print(f'Variable: {v}')
    dataset = measure_run(dataset=dataset, variable=v, n_periods=3, measure_type='std')














# Download S3 Bucket
def download_s3_folder(s3_client, s3_folder, local_dir, aws_bucket):
    import boto3
    from os import path, makedirs
    from botocore.exceptions import ClientError
    from boto3.exceptions import S3TransferFailedError

    def get_all_s3_objects(s3, **base_kwargs):
        continuation_token = None
        while True:
            list_kwargs = dict(MaxKeys=1000, **base_kwargs)
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token
            response = s3.list_objects_v2(**list_kwargs)
            yield from response.get('Contents', [])
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')

    all_s3_objects_gen = get_all_s3_objects(s3_client, Bucket=aws_bucket)

    for obj in all_s3_objects_gen:
        source = obj['Key']
        if source.startswith(s3_folder):
            destination = path.join(local_dir, source)
            if not path.exists(path.dirname(destination)):
                makedirs(path.dirname(destination))
            try:
                s3_client.download_file(aws_bucket, source, destination)
            except (ClientError, S3TransferFailedError) as e:
                print('[ERROR] Could not download file "%s": %s' % (source, e))


download_s3_folder(s3_client=s3, s3_folder='pp_2022_10_02_itr_dre_2015.parquet',
                   local_dir=DIR_PATH, aws_bucket='fundamentus-pre-processed')

# Open dataframe
# dataset = sk.read.parquet(os.path.join(DIR_PATH_PROCESSED_ITR, 'pp_itr_dre_2013.parquet'))
