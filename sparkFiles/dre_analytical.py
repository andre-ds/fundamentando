import os
from pyspark.sql import SparkSession
from sparkDocuments import DIR_S3_ANALYTICAL
from PreProcessing import PreProcessing


if __name__ == "__main__":

    sk = SparkSession.builder.getOrCreate()
    pp = PreProcessing(spark_environment=sk)

    dataset = sk.read.parquet(os.path.join(DIR_S3_ANALYTICAL, f'pp_dre_union.parquet'))


    # Calculating Lag and percentage change
    variable_list_lag = ['amt_sales_revenue', 'amt_cost_goods_and_services',
                        'amt_earnings_before_interest_and_taxes', 'amt_financial_results',
                        'amt_net_profit']
    for v in variable_list_lag:
        print(f'Lag-Variable: {v}')
        dataset = pp.get_lag(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], type='amt', measure='tot', variable=v, nlag=1, period='m')
        dataset = pp.get_lag(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], type='amt', measure='tot', variable=v, nlag=4, period_text='1y')

    for v in variable_list_lag:
        print(f'Pct-Variable: {v}')
        dataset = pp.get_percentage_change(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, nlag=1, period='m')
        dataset = pp.get_percentage_change(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, nlag=4, period_text='1y')

    # Mean Beetween rows
    variable_list_rows = ['amt_sales_revenue', 'amt_cost_goods_and_services',
                        'amt_earnings_before_interest_and_taxes', 'amt_financial_results',
                        'amt_net_profit']
    for v in variable_list_rows:
        print(f'Mean-Variable: {v}')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, n_periods=1, measure_type='mean')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, n_periods=2, measure_type='mean')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, n_periods=3, measure_type='mean')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, n_periods=4, measure_type='mean')

    for v in variable_list_rows:
        print(f'Std-Variable: {v}')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, n_periods=1, measure_type='std')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, n_periods=2, measure_type='std')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, n_periods=3, measure_type='std')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, n_periods=4, measure_type='std')

    # Beta 
    variable_beta = ['amt_sales_revenue', 'amt_cost_goods_and_services',
                        'amt_earnings_before_interest_and_taxes', 'amt_financial_results',
                        'amt_net_profit']
    for v in variable_beta:
        print(f'Beta-3-Variable: {v}')
        dataset = pp.measure_beta(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, n_periods=2)
    for v in variable_beta:
        print(f'Beta-6-Variable: {v}')
        dataset = pp.measure_beta(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, n_periods=5)
    for v in variable_beta:
        print(f'Beta-12-Variable: {v}')
        dataset = pp.measure_beta(dataset=dataset, partition_var=['id_cvm'], order_var=['dt_year', 'dt_quarter'], variable=v, n_periods=11)


    # Saving
    print('saving')
    dataset.write.format('parquet') \
        .mode('overwrite') \
       .save(os.path.join(DIR_S3_ANALYTICAL, 'analytical_dre.parquet'))  
