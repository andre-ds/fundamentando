from sparkFiles.PreProcessing import PreProcessing
from sparkFiles.sparkDocuments import schema_dre
import boto3
import os
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
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
import numpy as np
#from pyspark.sql import functions as F

DIR_PATH = os.path.join(os.path.dirname(os.path.realpath('__file__')), 'datalake')
DIR_PATH_PROCESSED_ITR = os.path.join(DIR_PATH, 'pre-processed-itr')
DIR_PATH_PROCESSED_DFP = os.path.join(DIR_PATH, 'pre-processed-dfp')


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
def get_lag(dataset, type, measure, variable, nlag, period=None, period_text=None):

    windowSpec  = Window.partitionBy('id_cvm').orderBy(['dt_year','dt_quarter'])

    if period_text is not None:
        dataset = dataset.withColumn(f'{type}_{measure}_{variable}_{period_text}_lag',lag(variable, nlag).over(windowSpec))
    else:
        dataset = dataset.withColumn(f'{type}_{measure}_{variable}_{nlag}{period}_lag',lag(variable, nlag).over(windowSpec))

    return dataset


def get_percentage_change(dataset, variable, nlag, period=None, period_text=None):

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

    def _mean_func(x):
        result = np.mean(x)
        return float(result)  

    def _median_func(x):
        result = np.median(x)
        return float(result)   

    def _std_func(x):
        result = np.std(x)
        return float(result)

    def _var_func(x):
        
        result = np.var(x)
        return float(result)

    def _min_func(x):
        
        result = np.min(x)
        return float(result)

    def _max_func(x):
        
        result = np.max(x)
        return float(result)

    
    if measure_type == 'mean':
        variable_name = f'amt_avg_{variable}_{n_periods}'
        func_udf = udf(_mean_func, DoubleType())
    elif measure_type == 'median':
        variable_name = f'amt_mda_{variable}_{n_periods}'
        func_udf = udf(_median_func, DoubleType())
    elif measure_type == 'std':
        variable_name = f'amt_std_{variable}_{n_periods}'
        func_udf = udf(_var_func, DoubleType())
    elif measure_type == 'var':
        variable_name = f'amt_var_{variable}_{n_periods}'
        func_udf = udf(_std_func, DoubleType())
    elif measure_type == 'min':
        variable_name = f'amt_min_{variable}_{n_periods}'
        func_udf = udf(_min_func, DoubleType())
    elif measure_type == 'max':
        variable_name = f'amt_max_{variable}_{n_periods}'
        func_udf = udf(_max_func, DoubleType())

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


def measure_beta(dataset, variable, n_periods):

    def _beta(x, y, deg=1):
        try:    
            coef = np.polyfit(x, y, deg=deg)
            coef = coef[-1]    
            return float(coef)
        except:
            return float(np.nan)

    
    variable_name = f'amt_beta_{variable}_{n_periods}'
    func_udf = udf(_beta, DoubleType())

    windowSpec  = Window.partitionBy('id_cvm').orderBy(['dt_year','dt_quarter'])

    df = (
        dataset
        .orderBy(['id_cvm','dt_year','dt_quarter'])
        .withColumn('y_array', row_number().over(windowSpec))
        .groupBy('id_cvm')
        .agg(collect_list(variable).alias('x_array'), collect_list('y_array').alias('y_array'))
    )

    dataset = (
        dataset
        .withColumn('row_n', row_number().over(windowSpec))
        .join(df, on='id_cvm', how='left')
        .withColumn('x_array_slice', slice(col('x_array'), col('row_n'), n_periods))
        .withColumn('y_array_slice', slice(col('y_array'), col('row_n'), n_periods))
        .withColumn(variable_name, func_udf(col('x_array_slice'), col('y_array_slice')))
        .drop(*['row_n', 'x_array', 'x_array_slice', 'y_array', 'y_array_slice'])
    )

    return dataset


# Calculating Lag and percentage change
variable_list_lag = ['sales_revenue', 'cost_goods_and_services',
                     'earnings_before_interest_and_taxes', 'financial_results',
                     'net_profit']
for v in variable_list_lag:
    dataset = get_lag(dataset=dataset, type='amt', measure='tot', variable=v, nlag=1, period='m')
    dataset = get_lag(dataset=dataset, type='amt', measure='tot', variable=v, nlag=4, period_text='1y')
    dataset = get_percentage_change(dataset=dataset, variable=v, nlag=1, period='m')
    dataset = get_percentage_change(dataset=dataset, variable=v, nlag=4, period_text='1y')

# Mean Beetween rows
variable_list_rows = ['sales_revenue', 'cost_goods_and_services',
                     'earnings_before_interest_and_taxes', 'financial_results',
                     'net_profit']
for v in variable_list_rows:
    print(f'Variable: {v}')
    dataset = measure_run(dataset=dataset, variable=v, n_periods=3, measure_type='mean')
for v in variable_list_rows:
    print(f'Variable: {v}')
    dataset = measure_run(dataset=dataset, variable=v, n_periods=3, measure_type='std')

# Beta
variable_beta = ['sales_revenue', 'cost_goods_and_services',
                     'earnings_before_interest_and_taxes', 'financial_results',
                     'net_profit']
for v in variable_beta:
    print(f'Variable: {v}')
    dataset = measure_beta(dataset=dataset, variable=v, n_periods=3)
    dataset = measure_beta(dataset=dataset, variable=v, n_periods=6)
    dataset = measure_beta(dataset=dataset, variable=v, n_periods=12)


#dataset.toPandas().to_csv(os.path.join(DIR_PATH, 'teste.csv'))


