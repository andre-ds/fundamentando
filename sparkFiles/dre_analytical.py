import os
from pyspark.sql import SparkSession
from sparkDocuments import DIR_S3_ANALYTICAL_DRE
from PreProcessing import PreProcessing


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
