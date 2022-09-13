import os
from pyspark.sql.types import StructField, StructType, DateType, DoubleType, StringType, IntegerType, FloatType


PATH_DATALAKE = '/datalake'
DIR_PATH_RAW = os.path.join(PATH_DATALAKE, 'raw')
DIR_PATH_PROCESSED = os.path.join(PATH_DATALAKE, 'pre-processed')

types_dict = {
            'itr_dre':'itr_cia_aberta_DRE_con',
            'itr_bpp':'itr_cia_aberta_BPP_con',
            'itr_bpa':'itr_cia_aberta_BPA_con',
            'dfp_dre':'dfp_cia_aberta_DRE_con'
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

itr_bp_ba = StructType([
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