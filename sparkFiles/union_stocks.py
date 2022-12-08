import os
import argparse
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from sparkDocuments import DIR_PATH_RAW_STOCK, DIR_PATH_PROCESSED_STOCK


def union_stock(execution_date):
    
    from pyspark.sql.functions import col
    sk = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

    extract_at = execution_date.replace('-', '_')
    filename = f'extracted_{extract_at}_stock.parquet'
    dataset = sk.read.parquet(os.path.join(DIR_PATH_RAW_STOCK, filename))

    # Pre-processing

    dataset = (
        dataset
        .orderBy(col('date'))
        .dropDuplicates()
    )
    # Append Dataset
    dataset.write.format('parquet') \
            .partitionBy('date') \
            .mode('append') \
        .save(os.path.join(DIR_PATH_PROCESSED_STOCK, f'pp_stock_union.parquet'))  


if __name__ == "__main__":


    parser = argparse.ArgumentParser(
        description="Spark Pre-processing"
    )
    parser.add_argument("--execution_date", required=True)
    args = parser.parse_args()

    union_stock(execution_date=args.execution_date)
