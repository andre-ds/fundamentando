import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, abs, collect_list, row_number, slice, udf
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from sparkDocuments import DIR_S3_ANALYTICAL_DRE


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


if __name__ == "__main__":

    sk = SparkSession.builder.getOrCreate()

    dataset = sk.read.parquet(os.path.join(DIR_S3_ANALYTICAL_DRE, f'pp_dre_union.parquet'))


    # Calculating Lag and percentage change
    variable_list_lag = ['sales_revenue', 'cost_goods_and_services',
                        'earnings_before_interest_and_taxes', 'financial_results',
                        'net_profit']
    for v in variable_list_lag:
        print(f'Lag-Variable: {v}')
        dataset = get_lag(dataset=dataset, type='amt', measure='tot', variable=v, nlag=1, period='m')
        dataset = get_lag(dataset=dataset, type='amt', measure='tot', variable=v, nlag=4, period_text='1y')

    for v in variable_list_lag:
        print(f'Pct-Variable: {v}')
        dataset = get_percentage_change(dataset=dataset, variable=v, nlag=1, period='m')
        dataset = get_percentage_change(dataset=dataset, variable=v, nlag=4, period_text='1y')

    # Mean Beetween rows
    variable_list_rows = ['sales_revenue', 'cost_goods_and_services',
                        'earnings_before_interest_and_taxes', 'financial_results',
                        'net_profit']
    for v in variable_list_rows:
        print(f'Mean-Variable: {v}')
        dataset = measure_run(dataset=dataset, variable=v, n_periods=3, measure_type='mean')

    for v in variable_list_rows:
        print(f'Std-Variable: {v}')
        dataset = measure_run(dataset=dataset, variable=v, n_periods=3, measure_type='std')
    '''
    # Beta
    variable_beta = ['sales_revenue', 'cost_goods_and_services',
                        'earnings_before_interest_and_taxes', 'financial_results',
                        'net_profit']
    for v in variable_beta:
        print(f'Beta-3-Variable: {v}')
        dataset = measure_beta(dataset=dataset, variable=v, n_periods=3)
    for v in variable_beta:
        print(f'Beta-6-Variable: {v}')
        dataset = measure_beta(dataset=dataset, variable=v, n_periods=6)
    for v in variable_beta:
        print(f'Beta-12-Variable: {v}')
        dataset = measure_beta(dataset=dataset, variable=v, n_periods=12)
    '''

    # Saving
    print('saving')
    dataset.write.format('parquet') \
        .mode('overwrite') \
       .save(os.path.join(DIR_S3_ANALYTICAL_DRE, 'analytical_dre.parquet'))  
