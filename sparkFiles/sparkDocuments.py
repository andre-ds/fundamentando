import os
from pyspark.sql.types import StructField, StructType, DateType, DoubleType, StringType, IntegerType, FloatType


PATH_DATALAKE = '/datalake'
DIR_PATH_RAW_FCA= os.path.join(PATH_DATALAKE, 'raw-fca')
DIR_PATH_RAW_DFP = os.path.join(PATH_DATALAKE, 'raw-dfp')
DIR_PATH_RAW_ITR = os.path.join(PATH_DATALAKE, 'raw-itr')
DIR_PATH_RAW_STOCK = os.path.join(PATH_DATALAKE, 'raw-stock')
DIR_PATH_PROCESSED_FCA_GENERAL_REGISTER = os.path.join(PATH_DATALAKE, 'pre-processed-fca-general-register')
DIR_PATH_PROCESSED_FCA_STOCK_TYPE = os.path.join(PATH_DATALAKE, 'pre-processed-fca-stock-type')
DIR_PATH_PROCESSED_DFP = os.path.join(PATH_DATALAKE, 'pre-processed-dfp')
DIR_PATH_PROCESSED_ITR = os.path.join(PATH_DATALAKE, 'pre-processed-itr')
DIR_PATH_PROCESSED_STOCK = os.path.join(PATH_DATALAKE, 'pre-processed-stock')
DIR_PATH_ANALYTICAL = os.path.join(PATH_DATALAKE, 'analytical')

DIR_S3_RAW_DFP = 's3://fundamentus-raw-dfp'
DIR_S3_RAW_ITR = 's3://fundamentus-raw-itr'
DIR_S3_PRE_PROCESSED_ITR = 's3://fundamentus-pre-processed-itr'
DIR_S3_PRE_PROCESSED_DFP = 's3://fundamentus-pre-processed-dfp'
DIR_S3_PROCESSED_STOCKS = 's3://fundamentus-pre-processed-stock'
DIR_S3_ANALYTICAL = 's3://fundamentus-analytical'


types_dict = {
            'itr_dre':'itr_cia_aberta_DRE_con',
            'itr_bpp':'itr_cia_aberta_BPP_con',
            'itr_bpa':'itr_cia_aberta_BPA_con',
            'dfp_dre':'dfp_cia_aberta_DRE_con',
            'dfp_bpp':'dfp_cia_aberta_BPP_con',
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
            '1':'amt_total_assets',
            '1.01':'amt_current_assets',
            '1.01.01':'amt_cash_and_cash_equivalents',
            '1.01.04':'amt_stocks',
            '1.02':'amt_non_current_assets'
}

dre_account = {
            '3.01':'amt_sales_revenue',
            '3.02':'amt_cost_goods_and_services',
            '3.03':'amt_groos_revenue',
            '3.04':'amt_operating_revenues_and_expenses',
            '3.05':'amt_earnings_before_interest_and_taxes',
            '3.06':'amt_financial_results',
            '3.07':'amt_earnings_before_income_tax_and_social_contribution',
            '3.11':'amt_net_profit'
}


varlist_financial_information_analytical = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'dt_quarter', 'processed_at',
                'amt_cost_goods_and_services', 'amt_earnings_before_income_tax_and_social_contribution',
                'amt_earnings_before_interest_and_taxes', 'amt_financial_results', 'amt_groos_revenue',
                'amt_net_profit', 'amt_operating_revenues_and_expenses', 'amt_sales_revenue',
                'amt_cash_and_cash_equivalents', 'amt_current_assets', 'amt_non_current_assets',
                'amt_total_assets', 'amt_current_liabilities', 'amt_loans_credits',
                'amt_net_equity', 'amt_non_current_liabilities', 'amt_total_liabilities']


varlist_dre = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_refer', 'dt_ini_exerc', 'dt_fim_exerc', 'dt_year', 'dt_quarter','type_dre',
        'amt_sales_revenue', 'amt_cost_goods_and_services', 'amt_groos_revenue', 'amt_operating_revenues_and_expenses',
        'amt_selling_expenses', 'amt_general_administrative_expenses', 'amt_losses_non_recoverability_of_assets',
        'amt_other_operating_income', 'amt_other_operating_expenses', 'amt_equity_equivalence', 'amt_earnings_before_interest_and_taxes',
        'amt_financial_results', 'amt_financial_income', 'amt_financial_expenses', 'amt_earnings_before_income_tax_and_social_contribution',
        'amt_income_tax_social_contribution_on_profit', 'amt_net_profit', 'amt_financial_intermediation_income',
        'amt_financial_intermediation_expenses', 'amt_gross_income_financial_intermediation', 'amt_provision_for_expected_loss_expense_credit_risk',
        'amt_income_from_service_provision', 'amt_staff_costs', 'amt_other_administrative_expenses', 'amt_tax_expenses', 'amt_other_operational_expenses']

varlist_bpp = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_refer', 'dt_fim_exerc', 'dt_year', 'dt_quarter', 'cat_type_dre',
                'amt_total_liabilities', 'amt_current_liabilities', 'amt_non_current_liabilities', 'amt_loans_financing', 'amt_net_equity',
                'amt_provisions', 'amt_long_term_liabilities']

varlist_bpa = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_refer', 'dt_fim_exerc', 'dt_year', 'dt_quarter', 'cat_type_dre',
                'amt_total_assets', 'amt_current_assets', 'amt_cash_and_cash_equivalents', 'amt_non_current_assets', 'amt_finantial_assets', 'amt_investments',
                'amt_immobilized', 'amt_intangible']


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
    StructField('amt_cost_goods_and_services', FloatType(), True),
    StructField('amt_earnings_before_income_tax_and_social_contribution',
                FloatType(), True),
    StructField('amt_earnings_before_interest_and_taxes', FloatType(), True),
    StructField('amt_financial_results', FloatType(), True),
    StructField('amt_groos_revenue', FloatType(), True),
    StructField('amt_net_profit', FloatType(), True),
    StructField('amt_operating_revenues_and_expenses', FloatType(), True),
    StructField('amt_sales_revenue', FloatType(), True),
    StructField('processed_at', DateType(), True),
])

schema_fca_register = StructType([
    StructField('id_cnpj', StringType(), True),
    StructField('dt_refer', DateType(), True),
    StructField('txt_company_name', StringType(), True),
    StructField('dt_company_creation', DateType(), True),
    StructField('dt_cvm_register', DateType(), True),
    StructField('cat_cvm_register_situation', StringType(), True),
    StructField('dt_cvm_register_situation', DateType(), True),
    StructField('cat_sector', StringType(), True),
    StructField('text_sector', StringType(), True),
    StructField('cat_situation_issuer', StringType(), True),
    StructField('dt_situation_issuer', DateType(), True),
    StructField('dt_year_fiscal_end', IntegerType(), True),
    StructField('dt_month_fiscal_end', IntegerType(), True),
    StructField('cat_country_origin', StringType(), True),
    StructField('processed_at', DateType(), True)
])

schema_fca_stock_type = StructType([
    StructField('id_cnpj', StringType(), True),
    StructField('dt_refer', DateType(), True),
    StructField('id_document', StringType(), True),
    StructField('qty_version', IntegerType(), True),
    StructField('cat_stock_type', StringType(), True),
    StructField('cat_preferencial_stock', DateType(), True),
    StructField('id_ticker:', StringType(), True),
    StructField('cat_type_market', StringType(), True),
    StructField('cat_market', StringType(), True),
    StructField('text_market', StringType(), True),
    StructField('cat_governance_segmentation', DateType(), True),
    StructField('dt_start_negociation', DateType(), True),
    StructField('dt_end_negociation', DateType(), True),
    StructField('dt_start_listing', DateType(), True),
    StructField('dt_end_listing', DateType(), True),
    StructField('processed_at', DateType(), True)
])

schema_pp_bpa_bpp = StructType([
    StructField('id_cvm', StringType(), True),
    StructField('id_cnpj', StringType(), True),
    StructField('txt_company_name', StringType(), True),
    StructField('dt_refer', StringType(), True),
    StructField('dt_fim_exerc', IntegerType(), True),
    StructField('dt_year', IntegerType(), True),
    StructField('dt_quarter', IntegerType(), True),
    StructField('cat_type_dre', StringType(), True),
    StructField('processed_at', DateType(), True),
    StructField('amt_sales_revenue', FloatType(), True),
    StructField('amt_cost_goods_and_services', FloatType(), True),
    StructField('amt_groos_revenue', FloatType(), True),
    StructField('amt_operating_revenues_and_expenses', FloatType(), True),
    StructField('amt_selling_expenses', FloatType(), True),
    StructField('amt_general_administrative_expenses', FloatType(), True),
    StructField('amt_losses_non_recoverability_of_assets', FloatType(), True),
    StructField('amt_other_operating_income', FloatType(), True),
    StructField('amt_other_operating_expenses', FloatType(), True),
    StructField('amt_equity_equivalence', FloatType(), True),
    StructField('amt_earnings_before_interest_and_taxes', FloatType(), True),
    StructField('amt_financial_results', FloatType(), True),
    StructField('amt_financial_income', FloatType(), True),
    StructField('amt_financial_expenses', FloatType(), True),
    StructField('amt_earnings_before_income_tax_and_social_contribution', FloatType(), True),
    StructField('amt_income_tax_social_contribution_on_profit', FloatType(), True),
    StructField('amt_net_profit', FloatType(), True),
    StructField('amt_financial_intermediation_income', FloatType(), True),
    StructField('amt_financial_intermediation_expenses', FloatType(), True),
    StructField('amt_gross_income_financial_intermediation', FloatType(), True),
    StructField('amt_provision_for_expected_loss_expense_credit_risk', FloatType(), True),
    StructField('amt_income_from_service_provision', FloatType(), True),
    StructField('amt_staff_costs', FloatType(), True),
    StructField('amt_other_administrative_expenses', FloatType(), True),
    StructField('amt_tax_expenses', FloatType(), True),
    StructField('amt_other_operational_expenses', FloatType(), True),
    StructField('amt_total_assets', FloatType(), True),
    StructField('amt_current_assets', FloatType(), True),
    StructField('amt_cash_and_cash_equivalents', FloatType(), True),
    StructField('amt_non_current_assets', FloatType(), True),
    StructField('amt_finantial_assets', FloatType(), True),
    StructField('amt_investments', FloatType(), True),
    StructField('amt_immobilized', FloatType(), True),
    StructField('amt_intangible', FloatType(), True),
    StructField('amt_total_liabilities', FloatType(), True),
    StructField('amt_current_liabilities', FloatType(), True),
    StructField('amt_non_current_liabilities', FloatType(), True),
    StructField('amt_loans_financing', FloatType(), True),
    StructField('amt_net_equity', FloatType(), True),
    StructField('amt_provisions', FloatType(), True),
    StructField('amt_long_term_liabilities', FloatType(), True),
])


varlist_financial_information_analytical = ['id_cvm', 'id_cnpj', 'id_ticker', 'txt_company_name', 'cat_type_dre', 'dt_refer', 'dt_year', 'dt_quarter', 'dt_fim_exerc',
    'amt_sales_revenue', 'amt_cost_goods_and_services', 'amt_groos_revenue', 'amt_operating_revenues_and_expenses',
    'amt_selling_expenses', 'amt_general_administrative_expenses', 'amt_losses_non_recoverability_of_assets', 
    'amt_other_operating_income', 'amt_other_operating_expenses', 'amt_equity_equivalence', 'amt_earnings_before_interest_and_taxes',
    'amt_financial_results', 'amt_financial_income', 'amt_financial_expenses', 'amt_earnings_before_income_tax_and_social_contribution',
    'amt_income_tax_social_contribution_on_profit', 'amt_net_profit', 'amt_financial_intermediation_income', 'amt_financial_intermediation_expenses',
    'amt_gross_income_financial_intermediation', 'amt_provision_for_expected_loss_expense_credit_risk', 'amt_income_from_service_provision',
    'amt_staff_costs', 'amt_other_administrative_expenses', 'amt_tax_expenses', 'amt_other_operational_expenses', 'amt_total_assets',
    'amt_current_assets', 'amt_cash_and_cash_equivalents', 'amt_non_current_assets', 'amt_finantial_assets', 'amt_investments',
    'amt_immobilized', 'amt_intangible', 'amt_total_liabilities', 'amt_current_liabilities', 'amt_non_current_liabilities',
    'amt_loans_financing', 'amt_net_equity', 'amt_provisions', 'amt_long_term_liabilities', 'amt_adj_close', 'amt_dividends']


'''
varlist_financial_information_analytical = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'dt_quarter', 'processed_at',
                'amt_cost_goods_and_services', 'amt_earnings_before_income_tax_and_social_contribution',
                'amt_earnings_before_interest_and_taxes', 'amt_financial_results', 'amt_groos_revenue',
                'amt_net_profit', 'amt_operating_revenues_and_expenses', 'amt_sales_revenue',
                'amt_cash_and_cash_equivalents', 'amt_current_assets', 'amt_non_current_assets', 'amt_stocks',
                'amt_total_assets', 'amt_current_liabilities', 'amt_loans_credits',
                'amt_net_equity', 'amt_non_current_liabilities', 'amt_total_liabilities']
'''