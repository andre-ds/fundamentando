import os
import argparse
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from sparkDocuments import DIR_PATH_RAW_STOCK, DIR_PATH_PROCESSED_STOCK
from PreProcessing import PreProcessing

def union_stock(reference_date):
    
    from pyspark.sql.functions import col
    sk = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())
    pp = PreProcessing(spark_environment=sk)
    
    reference_date = pp.get_start(execution_date=reference_date)
    reference_date = reference_date.replace('-', '_')
    filename = f'extracted_{reference_date}_stock.parquet'
    dataset = sk.read.parquet(os.path.join(DIR_PATH_RAW_STOCK, filename))

    # Pre-processing
    dataset = (
        dataset
        .orderBy(col('dt_date'))
        .dropDuplicates()
        .select('dt_date', 'id_cnpj', 'id_ticker', 'amt_adj_close', 'amt_close', 'amt_high', 'amt_low', 'amt_open', 'qty_volume', 'amt_dividends', 'cat_stock_splits')
    )
    # Append Dataset
    dataset.write.format('parquet') \
            .mode('append') \
            .save(os.path.join(DIR_PATH_PROCESSED_STOCK, 'pp_stock_union.parquet'))  


if __name__ == "__main__":


    parser = argparse.ArgumentParser(
        description="Spark Pre-processing"
    )
    parser.add_argument("--reference_date", required=True)
    args = parser.parse_args()

    union_stock(reference_date=args.reference_date)
