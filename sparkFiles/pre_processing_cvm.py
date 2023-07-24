import os
import argparse
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame
from PreProcessing import PreProcessing
from sparkDocuments import schema_fca_register, schema_fca_stock_type, schema_dre, schema_bp_ba


if __name__ == "__main__":
  
    parser = argparse.ArgumentParser(
        description="Spark Pre-processing"
    )
    parser.add_argument("--dataType", required=True)
    parser.add_argument("--years_list", required=True)
    parser.add_argument("--execution_date", required=True)
    args = parser.parse_args()


    sk = SparkSession(SparkContext(conf=SparkConf())\
        .getOrCreate())
        
    pp = PreProcessing(spark_environment=sk)
    extract_at = args.execution_date

    if args.dataType == 'fca_aberta_geral':
        pp.pre_process_cvm(dataType=args.dataType, year=args.years_list, schema=schema_fca_register, execution_date=extract_at)
    elif args.dataType == 'fca_valor_mobiliario':
        pp.pre_process_cvm(dataType=args.dataType, year=args.years_list, schema=schema_fca_stock_type, execution_date=extract_at)
    elif args.dataType == 'dfp_dre':
        pp.pre_process_cvm(dataType=args.dataType, year=args.years_list, schema=schema_dre, execution_date=extract_at)
    elif args.dataType == 'dfp_bpp':
        pp.pre_process_cvm(dataType=args.dataType, year=args.years_list, schema=schema_bp_ba, execution_date=extract_at)
    elif args.dataType == 'dfp_bpa':
        pp.pre_process_cvm(dataType=args.dataType, year=args.years_list, schema=schema_bp_ba, execution_date=extract_at)
    elif args.dataType == 'dfp_dfc':
        pp.pre_process_cvm(dataType=args.dataType, year=args.years_list, schema=schema_dre, execution_date=extract_at)
    elif args.dataType == 'itr_dre':
        pp.pre_process_cvm(dataType=args.dataType, year=args.years_list, schema=schema_dre, execution_date=extract_at)
    elif args.dataType == 'itr_bpp':
        pp.pre_process_cvm(dataType=args.dataType, year=args.years_list, schema=schema_bp_ba, execution_date=extract_at)
    elif args.dataType == 'itr_bpa':
        pp.pre_process_cvm(dataType=args.dataType, year=args.years_list, schema=schema_bp_ba, execution_date=extract_at)
    elif args.dataType == 'itr_dfc':
        pp.pre_process_cvm(dataType=args.dataType, year=args.years_list, schema=schema_dre, execution_date=extract_at)
    elif args.dataType == 'itr_dfc_table':
        pp.pre_process_cvm(dataType=args.dataType, year=args.years_list, schema=schema_dre, execution_date=extract_at)