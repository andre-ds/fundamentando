import os
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from sparkDocuments import schema_ticker, FILES_STOCKS, DIR_S3_STOCKS, DIR_S3_PROCESSED_STOCKS


if __name__ == "__main__":
    sk = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

    # Create an empty RDD with empty schema
    dataset = sk.createDataFrame(data=sk.sparkContext.emptyRDD(), schema=schema_ticker)
    # Union Dataset's
    for file in FILES_STOCKS:
        df = sk.read.parquet(os.path.join(DIR_S3_STOCKS, file))
        dataset = dataset.union(df)

    # Remove Duplicates
    dataset = dataset.dropDuplicates()

    dataset.write.format('parquet') \
            .mode('overwrite') \
        .save(os.path.join(DIR_S3_PROCESSED_STOCKS, f'pp_stock_union.parquet'))  