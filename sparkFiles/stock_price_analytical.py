import os
from pyspark.sql import SparkSession
from sparkDocuments import DIR_S3_ANALYTICAL, DIR_S3_PROCESSED_STOCKS
from PreProcessing import PreProcessing
from pyspark.sql.functions import col, year, quarter, max
from pyspark.sql.window import Window


if __name__ == "__main__":


    sk = SparkSession.builder.getOrCreate()
    pp = PreProcessing(spark_environment=sk)

    dataset = sk.read.parquet(os.path.join(DIR_S3_ANALYTICAL, f'analytical_stock_price_temp.parquet'))

    n_periods = [10, 15, 20, 25, 30, 60, 90, 120, 160, 180, 360]
    for p in n_periods:
        dataset = pp.measure_beta(dataset=dataset, partition_var=['id_cnpj', 'ticker'], order_var=['date'], variable='adj_close', n_periods=p)

    # Broken stock price - High
    n_periods = [5, 10, 15, 20, 25, 30, 60, 90, 120]
    for p in n_periods:
        dataset = pp.broken_stock_high(dataset=dataset, variable='adj_close', partition_variables=['id_cnpj', 'ticker'], order_variables=['date'], n_periods=p)
    for p in n_periods:
        dataset = pp.broken_stock_high(dataset=dataset, variable='high_price', partition_variables=['id_cnpj', 'ticker'], order_variables=['date'], n_periods=p)
    
    # Broken stock price - Low
    for p in n_periods:
        dataset = pp.broken_stock_low(dataset=dataset, variable='adj_close', partition_variables=['id_cnpj', 'ticker'], order_variables=['date'], n_periods=p)
        dataset = pp.broken_stock_low(dataset=dataset, variable='low_price', partition_variables=['id_cnpj', 'ticker'], order_variables=['date'], n_periods=p)
    
    n_periods = [10, 15, 20, 25, 30, 60, 90, 120]
    for p in n_periods:
        dataset = pp.IFR(dataset=dataset, partition_var=['id_cnpj', 'ticker'], order_var=['date'], n_periods=p, variable='adj_close')

    dataset.write.format('parquet').mode('overwrite').save(os.path.join(DIR_S3_ANALYTICAL, 'analytical_stock_price.parquet'))