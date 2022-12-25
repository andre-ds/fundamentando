import os
from pyspark.sql.types import StructField, StructType, DateType, DoubleType, StringType, IntegerType, FloatType


PATH_DATALAKE = '/datalake'
DIR_PATH_RAW_DFP = os.path.join(PATH_DATALAKE, 'raw-dfp')
DIR_PATH_RAW_ITR = os.path.join(PATH_DATALAKE, 'raw-itr')
DIR_PATH_RAW_STOCK = os.path.join(PATH_DATALAKE, 'raw-stock')
DIR_PATH_PROCESSED_DFP = os.path.join(PATH_DATALAKE, 'pre-processed-dfp')
DIR_PATH_PROCESSED_ITR = os.path.join(PATH_DATALAKE, 'pre-processed-itr')
DIR_PATH_PROCESSED_STOCK = os.path.join(PATH_DATALAKE, 'pre-processed-stock')
DIR_PATH_ANALYTICAL = os.path.join(PATH_DATALAKE, 'analytical')


DIR_S3_PROCESSED_STOCKS = 's3://fundamentus-pre-processed-stock'
DIR_S3_ANALYTICAL = 's3://fundamentus-analytical'


types_dict = {
            'itr_dre':'itr_cia_aberta_DRE_con',
            'itr_bpp':'itr_cia_aberta_BPP_con',
            'itr_bpa':'itr_cia_aberta_BPA_con',
            'dfp_dre':'dfp_cia_aberta_DRE_con',
            'dfp_bpp':'itr_cia_aberta_BPP_con',
            'dfp_bpa':'dfp_cia_aberta_BPA_con'
        }


# Auxiliary codes
bpp_account = {
            '2':'amt_total_liabilities',
            '2.01':'amt_current_liabilities',
            '2.02':'amt_non_current_liabilities',
            '2.02.01':'amt_loans_credits',
            '2.03':'amt_net_equity'
}

bpa_account = {
            '1':'total_assets',
            '1.01':'current_assets',
            '1.01.01':'cash_and_cash_equivalents',
            '1.01.04':'stocks',
            '1.02':'non_current_assets'
}

dre_account = {
            '3.01':'sales_revenue',
            '3.02':'cost_goods_and_services',
            '3.03':'groos_revenue',
            '3.04':'operating_revenues_and_expenses',
            '3.05':'earnings_before_interest_and_taxes',
            '3.06':'financial_results',
            '3.07':'earnings_before_income_tax_and_social_contribution',
            '3.11':'net_profit'
}


schema_dre = StructType([
    StructField('CNPJ_CIA', StringType(), True),
    StructField('DT_REFER', StringType(), True),
    StructField('VERSAO', IntegerType(), True),
    StructField('DENOM_CIA', StringType(), True),
    StructField('CD_CVM', StringType(), True),
    StructField('GRUPO_DFP', StringType(), True),
    StructField('MOEDA', StringType(), True),
    StructField('ESCALA_MOEDA', StringType(), True),
    StructField('ORDEM_EXERC', StringType(), True),
    StructField('DT_INI_EXERC', StringType(), True),
    StructField('DT_FIM_EXERC', StringType(), True),
    StructField('CD_CONTA', StringType(), True),
    StructField('DS_CONTA', StringType(), True),
    StructField('VL_CONTA', FloatType(), True),
    StructField('ST_CONTA_FIXA', StringType(), True)
])

schema_bp_ba = StructType([
    StructField('CNPJ_CIA', StringType(), True),
    StructField('DT_REFER', StringType(), True),
    StructField('VERSAO', IntegerType(), True),
    StructField('DENOM_CIA', StringType(), True),
    StructField('CD_CVM', StringType(), True),
    StructField('GRUPO_DFP', StringType(), True),
    StructField('MOEDA', StringType(), True),
    StructField('ESCALA_MOEDA', StringType(), True),
    StructField('ORDEM_EXERC', StringType(), True),
    StructField('DT_FIM_EXERC', StringType(), True),
    StructField('CD_CONTA', StringType(), True),
    StructField('DS_CONTA', StringType(), True),
    StructField('VL_CONTA', FloatType(), True),
    StructField('ST_CONTA_FIXA', StringType(), True)
])

schema_ticker = StructType([
    StructField('ticker', StringType(), True),
    StructField('date', StringType(), True),
    StructField('open', FloatType(), True),
    StructField('high', FloatType(), True),
    StructField('low', FloatType(), True),
    StructField('close', FloatType(), True),
    StructField('adj_close', FloatType(), True),
    StructField('volume', IntegerType(), True),
    StructField('dividends', IntegerType(), True),
    StructField('stock_splits', IntegerType(), True)
])

# Create an empty RDD with empty schema
schema_pp_dre = StructType([
    StructField('id_cvm', StringType(), True),
    StructField('id_cnpj', StringType(), True),
    StructField('txt_company_name', StringType(), True),
    StructField('dt_refer', DateType(), True),
    StructField('dt_fim_exerc', DateType(), True),
    StructField('dt_ini_exerc', DateType(), True),
    StructField('dt_year', IntegerType(), True),
    StructField('dt_quarter', IntegerType(), True),
    StructField('cost_goods_and_services', FloatType(), True),
    StructField('earnings_before_income_tax_and_social_contribution',
                FloatType(), True),
    StructField('earnings_before_interest_and_taxes', FloatType(), True),
    StructField('financial_results', FloatType(), True),
    StructField('groos_revenue', FloatType(), True),
    StructField('net_profit', FloatType(), True),
    StructField('operating_revenues_and_expenses', FloatType(), True),
    StructField('sales_revenue', FloatType(), True),
    StructField('processed_at', DateType(), True),
])
