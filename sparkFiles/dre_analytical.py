import os
from pyspark.sql import SparkSession
from sparkDocuments import DIR_S3_ANALYTICAL_DRE
from PreProcessing import PreProcessing

'''
import numpy as np
from pyspark.sql.functions import col, lag, abs, mean, stddev, variance, min, max
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

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


    WindowSpec = Window.partitionBy(['id_cvm']).orderBy(col('dt_year'), col('dt_quarter')).rowsBetween(-n_periods, Window.currentRow)

    if measure_type == 'mean':
        variable_name = f'amt_avg_{variable}_{n_periods}'
        dataset = dataset.withColumn(variable_name, mean(col(variable)).over(WindowSpec))
    elif measure_type == 'std':
        variable_name = f'amt_std_{variable}_{n_periods}'
        dataset = dataset.withColumn(variable_name, stddev(col(variable)).over(WindowSpec))
    elif measure_type == 'var':
        variable_name = f'amt_var_{variable}_{n_periods}'
        dataset = dataset.withColumn(variable_name, variance(col(variable)).over(WindowSpec))
    elif measure_type == 'min':
        variable_name = f'amt_min_{variable}_{n_periods}'
        dataset = dataset.withColumn(variable_name, min(col(variable)).over(WindowSpec))
    elif measure_type == 'max':
        variable_name = f'amt_max_{variable}_{n_periods}'
        dataset = dataset.withColumn(variable_name, max(col(variable)).over(WindowSpec))

    return dataset


def measure_beta(dataset, variable, n_periods):

    windowSpec1  = Window.partitionBy('id_cvm').orderBy(['dt_year','dt_quarter'])
    windowSpec2 = Window.partitionBy(['id_cvm']).orderBy(['dt_year','dt_quarter']).rowsBetween(-n_periods, Window.currentRow)

    variable_name = f'amt_beta_{variable}_{n_periods+1}'

    dataset = (
        dataset
        .withColumn('y', lag(variable, 1).over(windowSpec1))
        .withColumn('y_mean', mean(col('y')).over(windowSpec2))
        .withColumn('y_y_mean', col(variable)-col('y_mean'))
        .withColumn('x_mean', mean(col(variable)).over(windowSpec2))
        .withColumn('x_x_mean', col(variable)-col('x_mean'))
        .withColumn('num', col('x_x_mean')*col('y_y_mean'))
        .withColumn('den', col('x_x_mean')*col('x_x_mean'))
        .withColumn(variable_name, col('num')/col('den'))
        .drop('y', 'y_mean', 'y_y_mean', 'x_mean', 'x_x_mean', 'num', 'den')
    )

    return dataset

'''
if __name__ == "__main__":

    sk = SparkSession.builder.getOrCreate()
    pp = PreProcessing(spark_environment=sk)

    dataset = sk.read.parquet(os.path.join(DIR_S3_ANALYTICAL_DRE, f'pp_dre_union.parquet'))


    # Calculating Lag and percentage change
    variable_list_lag = ['sales_revenue', 'cost_goods_and_services',
                        'earnings_before_interest_and_taxes', 'financial_results',
                        'net_profit']
    for v in variable_list_lag:
        print(f'Lag-Variable: {v}')
        dataset = pp.get_lag(dataset=dataset, type='amt', measure='tot', variable=v, nlag=1, period='m')
        dataset = pp.get_lag(dataset=dataset, type='amt', measure='tot', variable=v, nlag=4, period_text='1y')

    for v in variable_list_lag:
        print(f'Pct-Variable: {v}')
        dataset = pp.get_percentage_change(dataset=dataset, variable=v, nlag=1, period='m')
        dataset = pp.get_percentage_change(dataset=dataset, variable=v, nlag=4, period_text='1y')

    # Mean Beetween rows
    variable_list_rows = ['sales_revenue', 'cost_goods_and_services',
                        'earnings_before_interest_and_taxes', 'financial_results',
                        'net_profit']
    for v in variable_list_rows:
        print(f'Mean-Variable: {v}')
        dataset = pp.measure_run(dataset=dataset, variable=v, n_periods=1, measure_type='mean')
        dataset = pp.measure_run(dataset=dataset, variable=v, n_periods=2, measure_type='mean')
        dataset = pp.measure_run(dataset=dataset, variable=v, n_periods=3, measure_type='mean')
        dataset = pp.measure_run(dataset=dataset, variable=v, n_periods=4, measure_type='mean')

    for v in variable_list_rows:
        print(f'Std-Variable: {v}')
        dataset = pp.measure_run(dataset=dataset, variable=v, n_periods=1, measure_type='std')
        dataset = pp.measure_run(dataset=dataset, variable=v, n_periods=2, measure_type='std')
        dataset = pp.measure_run(dataset=dataset, variable=v, n_periods=3, measure_type='std')
        dataset = pp.measure_run(dataset=dataset, variable=v, n_periods=4, measure_type='std')

    # Beta 
    variable_beta = ['sales_revenue', 'cost_goods_and_services',
                        'earnings_before_interest_and_taxes', 'financial_results',
                        'net_profit']
    for v in variable_beta:
        print(f'Beta-3-Variable: {v}')
        dataset = pp.measure_beta(dataset=dataset, variable=v, n_periods=2)
    for v in variable_beta:
        print(f'Beta-6-Variable: {v}')
        dataset = pp.measure_beta(dataset=dataset, variable=v, n_periods=5)
    for v in variable_beta:
        print(f'Beta-12-Variable: {v}')
        dataset = pp.measure_beta(dataset=dataset, variable=v, n_periods=11)


    # Saving
    print('saving')
    dataset.write.format('parquet') \
        .mode('overwrite') \
       .save(os.path.join(DIR_S3_ANALYTICAL_DRE, 'analytical_dre.parquet'))  
