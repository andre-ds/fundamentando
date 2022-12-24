import os
from pyspark.sql import SparkSession
from sparkDocuments import DIR_S3_PROCESSED_STOCKS, DIR_S3_ANALYTICAL
from PreProcessing import PreProcessing
from pyspark.sql.functions import col, year, quarter, max
from pyspark.sql.window import Window


if __name__ == "__main__":


    sk = SparkSession.builder.getOrCreate()
    pp = PreProcessing(spark_environment=sk)

    dataset = sk.read.parquet(os.path.join(DIR_S3_PROCESSED_STOCKS, f'pp_stock_union.parquet'))

    dataset = (
        dataset
        .withColumn('dt_year', year('date'))
        .withColumn('dt_quarter', quarter('date'))
        .withColumnRenamed('high', 'high_price')
        .withColumnRenamed('low', 'low_price')
        .withColumn('amplitude_close_high_price', col('adj_close')-col('high_price'))
        .orderBy('id_cnpj', 'dt_year', 'dt_quarter')
    )
    
    n_periods = [5, 10, 15, 20, 25, 30, 60, 90, 120, 160, 180, 360]
    for p in n_periods:
        print(f'Period: {p}')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cnpj', 'ticker'], order_var=['date'], variable='adj_close', n_periods=p, measure_type='mean')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cnpj', 'ticker'], order_var=['date'], variable='adj_close', n_periods=p, measure_type='std')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cnpj', 'ticker'], order_var=['date'], variable='adj_close', n_periods=p, measure_type='min')
        dataset = pp.measure_run(dataset=dataset, partition_var=['id_cnpj', 'ticker'], order_var=['date'], variable='adj_close', n_periods=p, measure_type='max')
   
    # Calculating Variation
    n_periods = [5, 10, 15, 20, 25, 30, 60, 90, 120, 160, 180, 360]
    for p in n_periods:
        print(f'Period: {p}')
        dataset = pp.get_percentage_change(dataset=dataset, partition_var=['id_cnpj', 'ticker'], order_var=['date'], variable='adj_close', nlag=p, period='d')
    for p in n_periods:
        print(f'Period: {p}')
        dataset = pp.get_percentage_change(dataset=dataset, partition_var=['id_cnpj', 'ticker'], order_var=['date'], variable='high_price', nlag=p, period='d')
    for p in n_periods:
        print(f'Period: {p}')
        dataset = pp.get_percentage_change(dataset=dataset, partition_var=['id_cnpj', 'ticker'], order_var=['date'], variable='low_price', nlag=p, period='d')
    for p in n_periods:
        print(f'Period: {p}')
        dataset = pp.get_percentage_change(dataset=dataset, partition_var=['id_cnpj', 'ticker'], order_var=['date'], variable='amplitude_close_high_price', nlag=p, period='d')

    dataset.write.format('parquet').mode('overwrite').save(os.path.join(DIR_S3_ANALYTICAL, 'analytical_stock_price_temp.parquet'))