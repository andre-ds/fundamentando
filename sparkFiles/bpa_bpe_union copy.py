import os
from sparkFiles.PreProcessing import PreProcessing
from sparkFiles.sparkDocuments import DIR_PATH_PROCESSED_STOCK, DIR_PATH_PROCESSED_DFP, DIR_PATH_PROCESSED_ITR
from sparkFiles.sparkDocuments import schema_pp_bpa_bpp
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


sk = SparkSession.builder.getOrCreate()
pp = PreProcessing(spark_environment=sk)

varlist = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'dt_quarter', 'processed_at',
                'amt_cost_goods_and_services', 'amt_earnings_before_income_tax_and_social_contribution',
                'amt_earnings_before_interest_and_taxes', 'amt_financial_results', 'amt_groos_revenue',
                'amt_net_profit', 'amt_operating_revenues_and_expenses', 'amt_sales_revenue',
                'amt_cash_and_cash_equivalents', 'amt_current_assets', 'amt_non_current_assets',
                'stocks', 'amt_total_assets', 'amt_current_liabilities', 'amt_loans_credits',
                'amt_net_equity', 'amt_non_current_liabilities', 'amt_total_liabilities']

dataset = sk.createDataFrame(data=sk.sparkContext.emptyRDD(), schema=schema_pp_bpa_bpp)

DIR_PATH_PROCESSED_DFP = '/home/andre/Dropbox/projects/fundamentalista_pipeline/datalake/pre-processed-dfp'

type_file='dfp_all'
if type_file=='dfp_all':
    files_list = pp.files_list(type_file=type_file)
    DIR_PATH = DIR_PATH_PROCESSED_DFP
elif type_file=='itr_all':
    files_list = pp.files_list(type_file=type_file)
    DIR_PATH = DIR_PATH_PROCESSED_ITR


for file in files_list:
    print(file)
    dataset_dre= sk.read.parquet(os.path.join(DIR_PATH, file[0]))
    dataset_bpa= sk.read.parquet(os.path.join(DIR_PATH, file[1]))
    dataset_bpp= sk.read.parquet(os.path.join(DIR_PATH, file[2]))
    on_list = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'dt_quarter', 'processed_at']
    df = (
        dataset_dre
        .join(dataset_bpa, on=on_list, how='left')
        .join(dataset_bpp, on=on_list, how='left')
        .select(varlist)
    )
    dataset = dataset.union(df)
dataset = dataset.withColumn('id', f.regexp_replace(f.col('id_cnpj'), '[./-]', ''))

#dataset_stock = sk.read.parquet(os.path.join(DIR_PATH_PROCESSED_STOCK, 'pp_stock_union.parquet'))
dataset_stock = sk.read.parquet(os.path.join('/home/andre/Dropbox/projects/fundamentalista_pipeline/datalake/pre-processed-stock', 'pp_stock_union.parquet'))
dataset_stock = (
    dataset_stock
    .withColumn('dt_year', f.year(f.col('date')))
    .withColumn('dt_quarter', f.quarter(f.col('date')))
    .groupBy('id_cnpj', 'ticker' ,'dt_year', 'dt_quarter')
    .agg(f.max(f.col('date')).alias('date'))
    .join(dataset_stock, on=['id_cnpj', 'ticker', 'date'], how='inner')
    .withColumnRenamed('id_cnpj', 'id')
    .select('id', 'ticker','dt_year', 'dt_quarter', 'adj_close', 'dividends')
)

dataset = (
    dataset_stock   
    .join(dataset, on=['id', 'dt_year', 'dt_quarter'], how='inner')
)

if __name__ == 'main':

    