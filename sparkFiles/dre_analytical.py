import argparse
from operator import index
from datetime import date
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, sum, max


def __files_list():
    years_list = [*range(2010, date.today().year+1, 1)]
    files_list = []
    for y in years_list:
        zip_files = [f'pp_itr_dre_{y}.parquet', f'pp_dfp_dre_{y}.parquet']
        files_list.append(zip_files)

    return files_list


def _union_quarters(dataset_itr:DataFrame, dataset_dfp:DataFrame) -> DataFrame:
    
        # Agregations Q1, Q2 and Q3
        df_sum = dataset_itr.groupBy(['processed_at', 'id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year']).agg(
            lit(None).alias('dt_refer'),
            lit(None).alias('dt_fim_exerc'),
            lit(None).alias('dt_ini_exerc'),
            lit(None).alias('dt_quarter'),
            sum('cost_goods_and_services').alias('cost_goods_and_services'), \
            sum('earnings_before_income_tax_and_social_contribution').alias('earnings_before_income_tax_and_social_contribution'), \
            sum('earnings_before_interest_and_taxes').alias('earnings_before_interest_and_taxes'), \
            sum('financial_results').alias('financial_results'), \
            sum('groos_revenue').alias('groos_revenue'), \
            sum('net_profit').alias('net_profit'), \
            sum('operating_revenues_and_expenses').alias('operating_revenues_and_expenses'), \
            sum('sales_revenue').alias('sales_revenue'))

        # Changing sinal for sumation
        varlist = ['cost_goods_and_services', 'earnings_before_income_tax_and_social_contribution',
        'earnings_before_interest_and_taxes', 'financial_results', 'groos_revenue',
        'net_profit', 'operating_revenues_and_expenses', 'sales_revenue']
        for v in varlist:
            df_sum = df_sum.withColumn(v, -col(v))

        # Union and sum datasets
        dataset_q4 = dataset_dfp.union(df_sum.select(dataset_dfp.columns))
        dataset_q4 = dataset_q4.groupBy(['processed_at', 'id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year']).agg(
            max('dt_refer').alias('dt_refer'), \
            max('dt_fim_exerc').alias('dt_fim_exerc'), \
            max('dt_ini_exerc').alias('dt_ini_exerc'), \
            max('dt_quarter').alias('dt_quarter'), \
            sum('cost_goods_and_services').alias('cost_goods_and_services'), \
            sum('earnings_before_income_tax_and_social_contribution').alias('earnings_before_income_tax_and_social_contribution'), \
            sum('earnings_before_interest_and_taxes').alias('earnings_before_interest_and_taxes'), \
            sum('financial_results').alias('financial_results'), \
            sum('groos_revenue').alias('groos_revenue'), \
            sum('net_profit').alias('net_profit'), \
            sum('operating_revenues_and_expenses').alias('operating_revenues_and_expenses'), \
            sum('sales_revenue').alias('sales_revenue'))

        # Union with quarters datasets
        dataset = dataset_itr.union(dataset_q4.select(dataset_itr.columns))

        return dataset


def _pp_union_dre():
    
    import os
    from pyspark.context import SparkContext
    from pyspark.conf import SparkConf
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f
    from sparkDocuments import schema_pp_dre, DIR_S3_RAW_DFP,  DIR_S3_RAW_ITR, DIR_S3_ANALYTICAL_DRE
    #from PreProcessing import PreProcessing

    sk = SparkSession(SparkContext(conf=SparkConf())\
    .getOrCreate())


    # Append _union_quarters
    #pp = PreProcessing(spark_environment=sk)

    dataset = sk.createDataFrame(data=sk.sparkContext.emptyRDD(),schema=schema_pp_dre)

    files_list = __files_list()       
    for file in files_list:
        dataset_itr = sk.read.parquet(os.path.join(DIR_S3_RAW_ITR, file[0]))
        dataset_dfp = sk.read.parquet(os.path.join(DIR_S3_RAW_DFP, file[1]))
        df = _union_quarters(dataset_itr=dataset_itr, dataset_dfp=dataset_dfp)
        dataset = dataset.union(df)

    # Saving
    dataset.write.format('parquet') \
        .mode('overwrite') \
       .save(os.path.join(DIR_S3_ANALYTICAL_DRE, f'pp_dre.parquet'))  
 

if __name__ == "__main__":
  
    ''' 
    parser = argparse.ArgumentParser(
        description="Spark Pre-processing"
    )
    parser.add_argument("--start", required=True)
    args = parser.parse_args()
'''
    _pp_union_dre()