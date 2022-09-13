from pyspark.sql.types import StructField, StructType, DateType, DoubleType, StringType, IntegerType, FloatType

# dataType
dataType = ['registration','itr', 'dfp']

# Companies Registration
repository_registration = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv'

# DFP
repository_DFP = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'

# ITR
repository_ITR = 'http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/'

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

# Demonstração de Resultado Abrangente (DRA)
DTYPE_DFP_1 = {'CNPJ_CIA': 'object', 'VERSAO': 'int64', 'DENOM_CIA': 'object', 'CD_CVM': 'object', 'GRUPO_DFP': 'object',
               'MOEDA': 'object', 'ESCALA_MOEDA': 'object', 'ORDEM_EXERC': 'object', 'CD_CONTA': 'object', 'DS_CONTA': 'object', 'VL_CONTA': 'float64', 'ST_CONTA_FIXA': 'object'}
DATES_DFP_1 = ['DT_REFER', 'DT_INI_EXERC', 'DT_FIM_EXERC']

years_list = ['2011', '2012', '2013', '2014', '2015',
              '2016', '2017', '2018', '2019', '2020', '2021', '2022']

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

dtype = {'CNPJ_CIA':'object', 'VERSAO':'int', 'DENOM_CIA':'object', 'CD_CVM':'object', 'GRUPO_DFP':'object',
                            'MOEDA':'object', 'ESCALA_MOEDA':'object', 'ORDEM_EXERC':'object',
                            'CD_CONTA':'object', 'DS_CONTA':'object', 'VL_CONTA':'float', 'ST_CONTA_FIXA':'object',
                            'DT_REFER':'object', 'DT_INI_EXERC':'object', 'DT_FIM_EXERC':'object'}

modelRevenueVariables = ['RESULTADO_BRUTO_LAG_1', 'RESULTADO_BRUTO_LAG_3',
       'RESULTADO_BRUTO_LAG_12', 'SECTOR_RESULTADO_BRUTO',
       'SECTOR_RESULTADO_BRUTO_LAG_1', 'SECTOR_RESULTADO_BRUTO_LAG_3',
       'SECTOR_RESULTADO_BRUTO_LAG_12', 'RETURN_RESULTADO_BRUTO_LAG_1',
       'RETURN_RESULTADO_BRUTO_LAG_3', 'RETURN_RESULTADO_BRUTO_LAG_12',
       'RETURN_SECTOR_RESULTADO_BRUTO_LAG_1',
       'RETURN_SECTOR_RESULTADO_BRUTO_LAG_3',
       'RETURN_SECTOR_RESULTADO_BRUTO_LAG_12', 'ESTATAL', 'ESTATAL_HOLDING',
       'ESTRANGEIRO', 'ESTRANGEIRO_HOLDING', 'PRIVADO', 'PRIVADO_HOLDING']