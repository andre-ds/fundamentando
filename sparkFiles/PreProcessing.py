import os
import re
from datetime import date
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, udf
from pyspark.sql.types import StructField, StructType, DateType, DoubleType, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, quarter, to_date, month, year, when, to_date, asc, months_between, round, concat, lit, regexp_replace, mean, stddev, variance, sum, min, max, lag, collect_list, slice, row_number, abs
from pyspark.sql.window import Window
from pyspark.sql import functions as f


class PreProcessing():


    def __init__(self, spark_environment):
        self._run()
        self.spark_environment = spark_environment


    def _run(self):

        self.todaystr = re.sub('-', '_', str((date.today())))


    def remove_missing_values(self, dataset:DataFrame) -> DataFrame:

        missing_variables = {col: dataset.filter(dataset[col].isNull()).count() for col in dataset.columns}

        return missing_variables


    def patterning_text_var(self, dataset:DataFrame, variable:str) -> DataFrame:

        dataset[variable] = dataset[variable].str.lower().str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        dataset[variable] = dataset[variable].replace(r'\s+', '_', regex=True)

        return dataset


    def files_list(self, type_file):
        
        #years_list = [*range(2011, date.today().year+1, 1)]
        years_list = [*range(2011, 2023, 1)]
        files_list = []
        for y in years_list:
            if type_file == 'itr_dfp':
                zip_files = [f'pp_itr_dre_{y}.parquet', f'pp_dfp_dre_{y}.parquet']
            elif type_file == 'dfp_all':
                zip_files = [ f'pp_dfp_dre_{y}.parquet', f'pp_dfp_bpa_{y}.parquet', f'pp_dfp_bpp_{y}.parquet', f'pp_dfp_dfc_{y}.parquet']
            elif type_file == 'itr_all':
                zip_files = [ f'pp_itr_dre_{y}.parquet', f'pp_itr_bpa_{y}.parquet', f'pp_itr_bpp_{y}.parquet', f'pp_itr_dfc_{y}.parquet']
            files_list.append(zip_files)

        return files_list


    def get_start(self, execution_date):

        from datetime import datetime, timedelta
        start = datetime.strptime(execution_date, "%Y-%m-%d").date()
        start = start + timedelta(-1)
        start = start.strftime('%Y-%m-%d')

        return start


    def get_end(self, execution_date):

        from datetime import datetime, timedelta
        start = datetime.strptime(execution_date, "%Y-%m-%d").date()
        end = start + timedelta(1)
        end = end.strftime('%Y-%m-%d')
            
        return end


    def _union_quarters(self, dataset_itr:DataFrame, dataset_dfp:DataFrame) -> DataFrame:

        '''
        Em desuso
        '''
        # Agregations Q1, Q2 and Q3
        df_sum = dataset_itr.groupBy(['processed_at', 'id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year']).agg(
            lit(None).alias('dt_refer'),
            lit(None).alias('dt_fim_exerc'),
            lit(None).alias('dt_ini_exerc'),
            lit(None).alias('dt_quarter'),
            sum('amt_cost_goods_and_services').alias('amt_cost_goods_and_services'), \
            sum('amt_earnings_before_income_tax_and_social_contribution').alias('amt_earnings_before_income_tax_and_social_contribution'), \
            sum('amt_earnings_before_interest_and_taxes').alias('amt_earnings_before_interest_and_taxes'), \
            sum('amt_financial_results').alias('amt_financial_results'), \
            sum('amt_groos_revenue').alias('amt_groos_revenue'), \
            sum('amt_net_profit').alias('amt_net_profit'), \
            sum('amt_operating_revenues_and_expenses').alias('amt_operating_revenues_and_expenses'), \
            sum('amt_sales_revenue').alias('amt_sales_revenue'))

        # Changing sinal for sumation
        varlist = ['amt_cost_goods_and_services', 'amt_earnings_before_income_tax_and_social_contribution',
        'amt_earnings_before_interest_and_taxes', 'amt_financial_results', 'amt_groos_revenue',
        'amt_net_profit', 'amt_operating_revenues_and_expenses', 'amt_sales_revenue']
        for v in varlist:
            df_sum = df_sum.withColumn(v, -col(v))

        # Union and sum datasets
        dataset_q4 = dataset_dfp.union(df_sum.select(dataset_dfp.columns))
        dataset_q4 = dataset_q4.groupBy(['processed_at', 'id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year']).agg(
            max('dt_refer').alias('dt_refer'), \
            max('dt_fim_exerc').alias('dt_fim_exerc'), \
            max('dt_ini_exerc').alias('dt_ini_exerc'), \
            max('dt_quarter').alias('dt_quarter'), \
            sum('amt_cost_goods_and_services').alias('amt_cost_goods_and_services'), \
            sum('amt_earnings_before_income_tax_and_social_contribution').alias('amt_earnings_before_income_tax_and_social_contribution'), \
            sum('amt_earnings_before_interest_and_taxes').alias('amt_earnings_before_interest_and_taxes'), \
            sum('amt_financial_results').alias('amt_financial_results'), \
            sum('amt_groos_revenue').alias('amt_groos_revenue'), \
            sum('amt_net_profit').alias('amt_net_profit'), \
            sum('amt_operating_revenues_and_expenses').alias('amt_operating_revenues_and_expenses'), \
            sum('amt_sales_revenue').alias('amt_sales_revenue'))

        # Union with quarters datasets
        dataset = dataset_itr.union(dataset_q4.select(dataset_itr.columns))

        return dataset
    

    def _get_last_quarter(self, df_itr, df_dfp):

        df_itr = df_itr.drop('processed_at')
        df_dfp = df_dfp.drop('processed_at')

        id_varlist = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year'] 
        left_itr = (
            df_itr
            .groupBy(id_varlist)
            .count()
            .filter(f.col('count')==3)
            .select('id_cvm')
            .join(df_dfp.select('id_cvm').dropDuplicates(), on='id_cvm', how='inner')
            .dropDuplicates()
        )

        # All Quarters Union
        drop_list = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_refer', 'dt_ini_exerc', 'dt_fim_exerc', 'dt_year', 'dt_quarter', 'cat_type_dre']
        agg_varlist = df_itr.columns
        for i in drop_list:
            if i in agg_varlist:
                agg_varlist.remove(i)
        agg_funcs_1 = [f.sum(x).alias(f"{x}") for x in agg_varlist]
        df_sum_quarters = (
            left_itr
            .join(df_itr, on='id_cvm', how='left')
            .drop('dt_quarter', 'dt_refer', 'dt_ini_exerc', 'dt_fim_exerc')
            .groupBy('id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'cat_type_dre')
            .agg(*agg_funcs_1)
            .withColumn('dt_quarter', f.lit(None))
            .withColumn('dt_refer', f.lit(None))
            .withColumn('dt_ini_exerc', f.lit(None))
            .withColumn('dt_fim_exerc', f.lit(None))
        )
        select_varlist = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_refer', 'dt_ini_exerc', 'dt_fim_exerc', 'dt_year', 'dt_quarter', 'cat_type_dre']
        select_varlist.extend(agg_varlist)
        df_sum_quarters = (
            df_sum_quarters
            .select(select_varlist)
        )

        # Negativations Informations
        for v in df_sum_quarters.columns[len(drop_list):]:
            df_sum_quarters = df_sum_quarters.withColumn(v, -f.col(v))
        # Calculating Last Quarter Dataset
        df_quarter = (
            left_itr
            .join(df_dfp, on='id_cvm', how='left')
            .union(df_sum_quarters.select(df_dfp.columns))
            .drop('dt_refer', 'dt_ini_exerc', 'dt_fim_exerc', 'dt_quarter')
        )
        agg_func_2 = [f.sum(x).alias(f"{x}") for x in df_quarter.columns[5:]]
        df_quarter = (
            df_quarter
            .groupBy('id_cvm', 'id_cnpj', 'txt_company_name', 'cat_type_dre', 'dt_year')
            .agg(*agg_func_2)
        )

        # Briging Dates
        df_iden = (
            df_dfp
            .select('id_cvm', 'id_cnpj', 'txt_company_name', 'dt_refer', 'dt_ini_exerc', 'dt_fim_exerc', 'dt_year', 'dt_quarter', 'cat_type_dre')
        )
        df_quarter = (
            df_quarter
            .join(df_iden, how='left', on=['id_cvm', 'id_cnpj', 'txt_company_name', 'cat_type_dre', 'dt_year'])
            .withColumn('teste', f.add_months(f.col('dt_fim_exerc'), -2))
        )
        # Complete Dataset
        dataset = (
            df_itr
            .union(df_quarter.select(df_itr.columns))
            .orderBy('id_cnpj', 'dt_year', 'dt_quarter')
        )
        
        return dataset
        

    def _pre_processing_fca_valor_mobiliario(self, dataset):

        import pyspark.sql.functions as f

        for var in dataset.columns:
            dataset = dataset.withColumnRenamed(var, var.lower())

        variablesRename = [
            ['cnpj_companhia', 'id_cnpj'],
            ['id_documento', 'id_document'],
            ['versao', 'qty_version'],
            ['data_referencia', 'dt_refer'],
            ['valor_mobiliario', 'cat_stock_type'],
            ['sigla_classe_acao_preferencial', 'cat_preferencial_stock'],
            ['codigo_negociacao', 'id_ticker'],
            ['mercado', 'cat_type_market'],
            ['sigla_entidade_administradora', 'cat_market'],
            ['entidade_administradora', 'text_market'],
            ['segmento', 'cat_governance_segmentation'],
            ['data_inicio_negociacao', 'dt_start_negociation'],
            ['data_fim_negociacao', 'dt_end_negociation'],
            ['data_inicio_listagem', 'dt_start_listing'],
            ['data_fim_listagem', 'dt_end_listing']
        ]

        select_variables = []
        for v in variablesRename:
            select_variables.append(v[1])
            print(v[0])
            dataset = dataset.withColumnRenamed(v[0], v[1])

        def remove_accents(inputStr):
            import unicodedata

            nfkdStr = unicodedata.normalize('NFKD', inputStr)
            withOutAccents = u"".join([c for c in nfkdStr if not unicodedata.combining(c)])
            
            return withOutAccents

        udf1 = f.udf(lambda x:remove_accents(x),StringType()) 

        dataset = (
            dataset
            .select(select_variables)
            .withColumn('id_cnpj', f.regexp_replace(f.col('id_cnpj'), '[./-]', ''))
            .fillna(subset=['text_market', 'cat_governance_segmentation', 'cat_stock_type', 'cat_type_market'], value='')
            .withColumn('text_market', udf1(f.col('text_market')))
            .withColumn('cat_governance_segmentation', udf1(f.col('cat_governance_segmentation')))
            .withColumn('cat_stock_type', udf1(f.col('cat_stock_type')))
            .withColumn('cat_type_market', udf1(f.col('cat_type_market')))
            .withColumn('qty_version', f.col('qty_version').cast('integer'))
        )

        for v in ['cat_stock_type', 'text_market', 'cat_governance_segmentation', 'cat_type_market']:
            dataset = dataset.withColumn(v, f.lower(f.col(v)))

        for v in ['dt_refer', 'dt_start_negociation', 'dt_end_negociation', 'dt_start_listing', 'dt_end_listing']:
            dataset = dataset.withColumn(v, to_date(col(v), 'yyyy-MM-dd'))

        return dataset


    def pre_processing_ticker_list(self, dataset_1, dataset_2):

        import pyspark.sql.functions as f

        dataset = (
            dataset_1
            .union(dataset_2)
            .withColumn('size', f.length(f.col('id_ticker')))
            .filter(f.col('size')>=5)
            .filter((f.col('cat_type_market')=='bolsa')
                &((f.col('cat_stock_type')=='acoes ordinarias')
                |(f.col('cat_stock_type')=='acoes preferenciais')
                |(f.col('cat_stock_type')=='units')))
            .filter(dataset_1['dt_end_listing'].isNull())
            .withColumn('id_ticker', f.upper(f.col('id_ticker')))
            .select('id_cnpj', 'id_ticker')
            .withColumn('ticker_list', f.concat(f.col('id_ticker'), f.lit('.SA')))
            .dropDuplicates()
        )
        
        df_on = (
            dataset
            .filter(f.col('id_ticker').contains('11'))
            .withColumn('id_ticker', f.regexp_replace(f.col('id_ticker'), '11', '3'))
            .withColumn('ticker_list', f.regexp_replace(f.col('ticker_list'), '11', '3'))

        )
        df_pn = (
            dataset
            .filter(f.col('id_ticker').contains('11'))
            .withColumn('id_ticker', f.regexp_replace(f.col('id_ticker'), '11', '4'))
            .withColumn('ticker_list', f.regexp_replace(f.col('ticker_list'), '11', '4'))
        )


        dataset = (
            dataset
            .union(df_on)
            .union(df_pn)
            .dropDuplicates()
        )

        return dataset


    def _pre_processing_fca_aberta_geral(self, dataset):

        import pyspark.sql.functions as f

        def remove_accents(inputStr):
            import unicodedata

            nfkdStr = unicodedata.normalize('NFKD', inputStr)
            withOutAccents = u"".join([c for c in nfkdStr if not unicodedata.combining(c)])
            
            return withOutAccents
            
        for var in dataset.columns:
            print(var)
            dataset = dataset.withColumnRenamed(var, var.lower())

        variablesRename = [['cnpj_companhia', 'id_cnpj'],
                        ['data_referencia', 'dt_refer'],
                        ['id_documento', 'id_document'],
                        ['versao', 'qty_version'],
                        ['nome_empresarial','txt_company_name'],
                        ['data_constituicao', 'dt_company_creation'],
                        ['data_registro_cvm', 'dt_cvm_register'],
                        ['situacao_registro_cvm', 'cat_cvm_register_situation'],
                        ['data_situacao_registro_cvm', 'dt_cvm_register_situation'],
                        ['setor_atividade', 'cat_sector'],
                        ['descricao_atividade', 'text_sector'],
                        ['situacao_emissor', 'cat_situation_issuer'],
                        ['data_situacao_emissor', 'dt_situation_issuer'],
                        ['dia_encerramento_exercicio_social', 'dt_year_fiscal_end'],
                        ['mes_encerramento_exercicio_social', 'dt_month_fiscal_end'],
                        ['pais_origem', 'cat_country_origin']]

        select_variables = []
        for v in variablesRename:
            select_variables.append(v[1])
            dataset = dataset.withColumnRenamed(v[0], v[1])
        
        udf1 = f.udf(lambda x:remove_accents(x), StringType()) 
        dataset = (
            dataset
            .select(select_variables)
            .withColumn('id_cnpj', f.regexp_replace(f.col('id_cnpj'), '[./-]', ''))
            .withColumn('dt_month_fiscal_end', f.col('dt_month_fiscal_end').cast('integer'))
            .withColumn('dt_year_fiscal_end', f.col('dt_year_fiscal_end').cast('integer'))
            .fillna(subset=['cat_sector', 'text_sector','txt_company_name'], value='')
            .withColumn('cat_sector', udf1(f.col('cat_sector')))         
            .withColumn('text_sector', udf1(f.col('text_sector')))
            .withColumn('txt_company_name', udf1(f.col('txt_company_name')))
        )
  
        for v in ['txt_company_name', 'cat_cvm_register_situation', 'cat_sector', 'text_sector', 'cat_situation_issuer', 'cat_country_origin']:
            dataset = dataset.withColumn(v, f.lower(f.col(v)))

        for v in ['dt_refer', 'dt_company_creation', 'dt_cvm_register', 'dt_cvm_register_situation', 'dt_situation_issuer']:
            dataset = dataset.withColumn(v, to_date(col(v), 'yyyy-MM-dd'))

        return dataset


    def _pre_processing_dre(self, dataset:DataFrame, type:str) -> DataFrame:

        from unidecode import unidecode
        from pyspark.sql.types import StringType
        from sparkDocuments import varlist_dre

        def remove_accent(text):
            return unidecode(text)

        for var in dataset.columns:
            dataset = dataset.withColumnRenamed(var, var.lower())
        # Keeping last registers
        dataset = dataset.filter(f.col('ordem_exerc') == 'ÚLTIMO')
        ## Getting year and quarter
        for v in ['dt_refer', 'dt_fim_exerc', 'dt_ini_exerc']:
            dataset = dataset.withColumn(v, f.to_date(f.col(v), 'yyyy-MM-dd'))
        dataset = (
            dataset
            .withColumn('dt_months_exercise', f.round(f.months_between(f.col('dt_fim_exerc'), f.col('dt_ini_exerc'))))
            .withColumn('dt_year', f.year('dt_refer'))
            .withColumn('dt_quarter', f.quarter('dt_refer'))
            .withColumn('vl_conta', f.when(f.col('escala_moeda') == 'MIL', f.col('vl_conta')*1000).otherwise(f.col('vl_conta')))
        )
        if type == 'dfp_dre':
            dataset = (
                dataset
                .filter(f.col('dt_months_exercise') == 12)
            )
        elif type == 'itr_dre':
            dataset = (
                dataset
                .filter(f.col('dt_months_exercise') == 3)
            )

        ## Pre-processing account type
        remove_accent_udf = f.udf(remove_accent, StringType())
        dataset = (
            dataset
            .fillna(subset=['ds_conta'], value='')
            .withColumn('cd_conta', f.trim(f.col('cd_conta')))
            .withColumn('ds_conta', f.trim(f.col('ds_conta')))
            .withColumn('ds_conta', f.lower(f.col('ds_conta')))
            .withColumn('ds_conta', remove_accent_udf(f.col('ds_conta')))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '[,.\\\(\)\/-/&]', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+e\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+de\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+do\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+dos\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+da\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+das\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+por\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+', '_'))
        )
        df_ty = (
            dataset
            .withColumn('type_dre',
            f.when(((f.col('cd_conta')=='3.01')&(f.col('ds_conta')=='receitavendabens_eou_servicos')),'type_01')
            .when(((f.col('cd_conta')=='3.01')&(f.col('ds_conta')=='receitasintermediacao_financeira')),'type_02')
            .when(((f.col('cd_conta')=='3.01')&(f.col('ds_conta')=='receitasoperacoes')),'type_03')
            )
            .select('cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_refer', 'dt_fim_exerc', 'dt_ini_exerc', 'dt_year', 'dt_quarter', 'type_dre')
            .filter(f.col('type_dre').isNotNull())
            .dropDuplicates()
        )
        varlist_on = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_refer', 'dt_fim_exerc', 'dt_ini_exerc', 'dt_year', 'dt_quarter']
        dataset = (
            dataset
            .join(df_ty, on=varlist_on, how='left')
        )
        # General (type_01)
        dataset = (
            dataset
            .withColumn('amt_sales_revenue',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.01')), f.col('vl_conta')))
            .withColumn('amt_cost_goods_and_services',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.02')), f.col('vl_conta')))
            .withColumn('amt_groos_revenue',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.03')), f.col('vl_conta')))
            .withColumn('amt_operating_revenues_and_expenses',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.04')), f.col('vl_conta')))
            .withColumn('amt_selling_expenses',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.04.01')), f.col('vl_conta')))
            .withColumn('amt_general_administrative_expenses',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.04.02')), f.col('vl_conta')))
            .withColumn('amt_losses_non_recoverability_of_assets',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.04.03')), f.col('vl_conta')))
            .withColumn('amt_other_operating_income',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.04.04')), f.col('vl_conta')))
            .withColumn('amt_other_operating_expenses',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.04.05')), f.col('vl_conta')))
            .withColumn('amt_equity_equivalence',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.04.06')), f.col('vl_conta')))
            .withColumn('amt_earnings_before_interest_and_taxes',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.05')), f.col('vl_conta')))
            .withColumn('amt_financial_results',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.06')), f.col('vl_conta')))
            .withColumn('amt_financial_income',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.06.01')), f.col('vl_conta')))
            .withColumn('amt_financial_expenses',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.06.02')), f.col('vl_conta')))
            .withColumn('amt_earnings_before_income_tax_and_social_contribution',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.07')), f.col('vl_conta')))
            .withColumn('amt_income_tax_social_contribution_on_profit',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.08')), f.col('vl_conta')))
            .withColumn('amt_net_profit',
                f.when(((f.col('type_dre')=='type_01')&(f.col('cd_conta')=='3.11')), f.col('vl_conta')))
        )

        # Bank resultado_bruto_intermediacao_financeira (type_02)
        dataset = (
            dataset
            .withColumn('amt_financial_intermediation_income',
                f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.01')), f.col('vl_conta')))
            .withColumn('amt_financial_intermediation_expenses',
                f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.02')), f.col('vl_conta')))
            .withColumn('amt_operating_revenues_and_expenses',
                f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.04')), f.col('vl_conta')).otherwise(f.col('amt_operating_revenues_and_expenses')))
            .withColumn('amt_gross_income_financial_intermediation',
                f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.03')), f.col('vl_conta')))
            #.withColumn('amt_provision_for_expected_loss_expense_credit_risk',
            #    f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.04.01')), f.col('vl_conta')))
            #.withColumn('amt_income_from_service_provision',
            #    f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.04.02')), f.col('vl_conta')))
            #.withColumn('amt_other_administrative_expenses',
            #    f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.04.04')), f.col('vl_conta')))
            #.withColumn('amt_other_operating_income',
            #    f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.04.06')), f.col('vl_conta')).otherwise(f.col('amt_other_operating_income')))
            #.withColumn('amt_other_operational_expenses',
            #    f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.04.07')), f.col('vl_conta')))
            #.withColumn('amt_equity_equivalence',
            #    f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.04.08')), f.col('vl_conta')).otherwise(f.col('amt_equity_equivalence')))
            #.withColumn('amt_staff_costs',
            #    f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.04.03')), f.col('vl_conta')))
            .withColumn('amt_earnings_before_income_tax_and_social_contribution',
                f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.05')), f.col('vl_conta')).otherwise(f.col('amt_earnings_before_income_tax_and_social_contribution')))
            .withColumn('amt_income_tax_social_contribution_on_profit',
                f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.06')), f.col('vl_conta')).otherwise(f.col('amt_income_tax_social_contribution_on_profit')))
            .withColumn('amt_net_profit',
                f.when(((f.col('type_dre')=='type_02')&((f.col('cd_conta')=='3.11')&(f.col('ds_conta')=='lucroprejuizo_consolidadoperiodo'))), f.col('vl_conta'))
                 .when(((f.col('type_dre')=='type_02')&((f.col('cd_conta')!='3.11')&(f.col('ds_conta')=='lucroprejuizo_consolidadoperiodo'))), f.col('vl_conta')).otherwise(f.col('amt_net_profit')))
            #.withColumn('amt_net_profit',
            #    f.when(((f.col('type_dre')=='type_02')&(f.col('cd_conta')=='3.11')), f.col('vl_conta')).otherwise(f.col('amt_net_profit')))
        )

        # type_03
        dataset = (
            dataset
            .withColumn('amt_sales_revenue', f.when((f.col('ds_conta').rlike('^receitasoperacoes$'))&(f.col('vl_conta')!=0)&(f.col('type_dre')=='type_03'), f.col('vl_conta'))
                .when((f.col('ds_conta').rlike('^outras_receitasdespesas_operacionais$'))&(f.col('type_dre')=='type_03'), f.col('vl_conta')).otherwise(f.col('amt_sales_revenue')))
            #.withColumn('amt_sales_revenue', f.when((f.col('ds_conta').rlike('outras_receitasdespesas_operacionais|^receitasoperacoes$'))&(f.col('type_dre')=='type_03'), f.col('vl_conta')).otherwise(f.col('amt_sales_revenue')))
            .withColumn('amt_cost_goods_and_services', f.when((f.col('ds_conta').rlike('custoservicos_prestados'))&(f.col('type_dre')=='type_03'), f.col('vl_conta')).otherwise(f.col('amt_cost_goods_and_services')))
            #.withColumn('amt_staff_costs',
            #    f.when(((f.col('type_dre')=='type_03')&(f.col('cd_conta')=='3.05.03')), f.col('vl_conta')))
            #.withColumn('amt_other_administrative_expenses', f.when((f.col('cd_conta')=='3.05')&(f.col('type_dre')=='type_03'), f.col('vl_conta')).otherwise(f.col('amt_other_administrative_expenses')))
            .withColumn('amt_equity_equivalence', f.when((f.col('cd_conta')=='3.06'), f.col('vl_conta')).otherwise(f.col('amt_equity_equivalence')))
            .withColumn('amt_earnings_before_interest_and_taxes', f.when((f.col('ds_conta').rlike('resultado_antesresultado_financeirodos_tributos'))&(f.col('type_dre')=='type_03'), f.col('vl_conta')).otherwise(f.col('amt_earnings_before_interest_and_taxes')))
            .withColumn('amt_earnings_before_income_tax_and_social_contribution', f.when((f.col('ds_conta').rlike('resultado_antestributos_sobre_o_lucro'))&(f.col('type_dre')=='type_03'), f.col('vl_conta')).otherwise(f.col('amt_earnings_before_income_tax_and_social_contribution')))
            .withColumn('amt_income_tax_social_contribution_on_profit', f.when((f.col('ds_conta').rlike('^impostorendacontribuicao_social_sobre_o_lucro$'))&(f.col('type_dre')=='type_03'), f.col('vl_conta')).otherwise(f.col('amt_income_tax_social_contribution_on_profit')))
            .withColumn('amt_net_profit', f.when((f.col('ds_conta').rlike('lucroprejuizo_consolidadoperiodo'))&(f.col('type_dre')=='type_03'), f.col('vl_conta')).otherwise(f.col('amt_net_profit')))
        )
        
        # Rename all variables to upercase
        variablesRename = [['cd_cvm', 'id_cvm'], ['cnpj_cia', 'id_cnpj'], ['codigo_cvm', 'id_cvm'], ['denom_cia','txt_company_name']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])
        
        # Standardizing id_cnpj
        dataset = dataset.withColumn('id_cnpj', f.regexp_replace(f.col('id_cnpj'), '[./-]', ''))

        # Genereting Copy
        #dataset_ds_conta = dataset
        dataset_ds_conta = dataset
        dataset_ds_conta = dataset_ds_conta.withColumnRenamed('type_dre', 'cat_type_dre')

        dataset = (
            dataset
            .groupBy(varlist_dre[0:9])
            .agg(
                *[f.sum(c).alias(c) for c in varlist_dre[9:]]
            )
            .withColumnRenamed('type_dre', 'cat_type_dre')
        )

        return dataset, dataset_ds_conta


    def _pre_processing_bpa(self, dataset:DataFrame) -> DataFrame:

        from unidecode import unidecode
        from pyspark.sql.types import StringType
        from sparkDocuments import varlist_bpa

        def remove_accent(text):
            return unidecode(text)


        # Pre-processing
        for var in dataset.columns:
            dataset = dataset.withColumnRenamed(var, var.lower())
        # Keeping las registers
        dataset = dataset.filter(f.col('ordem_exerc') == 'ÚLTIMO')
        ## Getting year and quarter
        for v in ['dt_refer', 'dt_fim_exerc']:
            dataset = dataset.withColumn(v, f.to_date(f.col(v), 'yyyy-MM-dd'))
        dataset = dataset.withColumn('dt_year', f.year('dt_refer'))
        dataset = dataset.withColumn('dt_quarter', f.quarter('dt_refer'))
        ## Standarting ESCALA_MOEDA
        dataset = dataset.withColumn('vl_conta', f.when(f.col('escala_moeda') == 'MIL', f.col('vl_conta')*1000).otherwise(f.col('vl_conta')))

        ## Pre-processing account type
        remove_accent_udf = f.udf(remove_accent, StringType())

        dataset = (
            dataset
            .withColumn('cd_conta', f.trim(f.col('cd_conta')))
            .withColumn('ds_conta', f.trim(f.col('ds_conta')))
            .withColumn('ds_conta', f.lower(f.col('ds_conta')))
            .withColumn('ds_conta', remove_accent_udf(f.col('ds_conta')))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '[,.\\\(\)\/-/&]', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+e\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+de\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+do\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+dos\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+da\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+das\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+por\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+', '_'))
        )

        df_ty = (
            dataset
            .withColumn('cat_type_dre',
            f.when(((f.col('cd_conta')=='1.01')&(f.col('ds_conta')=='ativo_circulante')),'type_01')
            .when(((f.col('cd_conta')=='1.01')&(f.col('ds_conta')=='caixaequivalentescaixa')), 'type_02')
            .when(((f.col('cd_conta')=='1.01.01')&(f.col('ds_conta')=='caixaequivalentescaixa')),'type_03')
            )
            .select('cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_refer', 'dt_fim_exerc', 'dt_year', 'dt_quarter', 'cat_type_dre')
            .filter(f.col('cat_type_dre').isNotNull())
            .dropDuplicates()
        )

        varlist_on = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_refer', 'dt_fim_exerc', 'dt_year', 'dt_quarter']
        dataset = (
            dataset
            .join(df_ty, on=varlist_on, how='left')
        )

        # Type 01
        dataset = (
            dataset
            .withColumn('amt_total_assets', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='1'), f.col('vl_conta')))
            .withColumn('amt_current_assets', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='1.01'), f.col('vl_conta')))
            .withColumn('amt_cash_and_cash_equivalents', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='1.01.01'), f.col('vl_conta')))
            .withColumn('amt_non_current_assets', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='1.02'), f.col('vl_conta')))
            .withColumn('amt_investments', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='1.02.02'), f.col('vl_conta')))
            .withColumn('amt_immobilized', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='1.02.03'), f.col('vl_conta')))
            .withColumn('amt_intangible', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='1.02.04'), f.col('vl_conta')))
        )

        # Type 02
        dataset = (
            dataset
            .withColumn('amt_total_assets', f.when((f.col('cat_type_dre')=='type_02')&(f.col('cd_conta')=='1'), f.col('vl_conta')).otherwise(f.col('amt_total_assets')))
            .withColumn('amt_cash_and_cash_equivalents', f.when((f.col('cat_type_dre')=='type_02')&(f.col('cd_conta')=='1.01'), f.col('vl_conta')).otherwise(f.col('amt_cash_and_cash_equivalents')))
            .withColumn('amt_finantial_assets', f.when((f.col('cat_type_dre')=='type_02')&(f.col('cd_conta')=='1.02'), f.col('vl_conta')))
            .withColumn('amt_investments', f.when((f.col('cat_type_dre')=='type_02')&(f.col('cd_conta')=='1.05'), f.col('vl_conta')).otherwise(f.col('amt_investments')))
            .withColumn('amt_immobilized', f.when((f.col('cat_type_dre')=='type_02')&(f.col('cd_conta')=='1.06'), f.col('vl_conta')).otherwise(f.col('amt_immobilized')))
            .withColumn('amt_intangible', f.when((f.col('cat_type_dre')=='type_02')&(f.col('cd_conta')=='1.07'), f.col('vl_conta')).otherwise(f.col('amt_intangible')))
        )

        # Type 03
        dataset = (
            dataset
            .withColumn('amt_total_assets', f.when((f.col('cat_type_dre')=='type_03')&(f.col('cd_conta')=='1'), f.col('vl_conta')).otherwise(f.col('amt_total_assets')))
            .withColumn('amt_current_assets', f.when((f.col('cat_type_dre')=='type_03')&(f.col('cd_conta')=='1.01'), f.col('vl_conta')).otherwise(f.col('amt_current_assets')))
            .withColumn('amt_cash_and_cash_equivalents', f.when((f.col('cat_type_dre')=='type_03')&(f.col('cd_conta')=='1.01.01'), f.col('vl_conta')).otherwise(f.col('amt_cash_and_cash_equivalents')))
            .withColumn('amt_non_current_assets', f.when((f.col('cat_type_dre')=='type_03')&(f.col('cd_conta')=='1.02'), f.col('vl_conta')).otherwise(f.col('amt_non_current_assets')))
            .withColumn('amt_investments', f.when((f.col('cat_type_dre')=='type_03')&(f.col('cd_conta')=='1.02.02'), f.col('vl_conta')).otherwise(f.col('amt_investments')))
            .withColumn('amt_immobilized', f.when((f.col('cat_type_dre')=='type_03')&(f.col('cd_conta')=='1.02.03'), f.col('vl_conta')).otherwise(f.col('amt_immobilized')))
        )

        # Rename all variables to upercase
        variablesRename = [['cd_cvm', 'id_cvm'], ['cnpj_cia', 'id_cnpj'], ['codigo_cvm', 'id_cvm'], ['denom_cia','txt_company_name']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])
        # Standardizing id_cnpj
        dataset = dataset.withColumn('id_cnpj', f.regexp_replace(f.col('id_cnpj'), '[./-]', ''))
        dataset = (
            dataset
            .select(*varlist_bpa)
            .groupBy(varlist_bpa[0:8])
            .agg(
                *[f.sum(c).alias(c) for c in varlist_bpa[8:]]
            )
        )

        dataset.columns
        return dataset


    def _pre_processing_bpp(self, dataset:DataFrame) -> DataFrame:
    
        from unidecode import unidecode
        from pyspark.sql.types import StringType
        from sparkDocuments import varlist_bpp


        def remove_accent(text):
            return unidecode(text)


        # Pre-processing
        for var in dataset.columns:
            dataset = dataset.withColumnRenamed(var, var.lower())
        # Keeping las registers
        dataset = dataset.filter(f.col('ordem_exerc') == 'ÚLTIMO')
        ## Getting year and quarter
        for v in ['dt_refer', 'dt_fim_exerc']:
            dataset = dataset.withColumn(v, f.to_date(f.col(v), 'yyyy-MM-dd'))
        dataset = dataset.withColumn('dt_year', f.year('dt_refer'))
        dataset = dataset.withColumn('dt_quarter', f.quarter('dt_refer'))
        ## Standarting ESCALA_MOEDA
        dataset = dataset.withColumn('vl_conta', f.when(f.col('escala_moeda') == 'MIL', f.col('vl_conta')*1000).otherwise(f.col('vl_conta')))

        ## Pre-processing account type
        remove_accent_udf = f.udf(remove_accent, StringType())

        dataset = (
            dataset
            .withColumn('cd_conta', f.trim(f.col('cd_conta')))
            .withColumn('ds_conta', f.trim(f.col('ds_conta')))
            .withColumn('ds_conta', f.lower(f.col('ds_conta')))
            .withColumn('ds_conta', remove_accent_udf(f.col('ds_conta')))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '[,.\\\(\)\/-/&]', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+e\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+de\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+do\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+dos\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+da\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+das\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+por\s+', ''))
            .withColumn('ds_conta', f.regexp_replace(f.col('ds_conta'), '\s+', '_'))
        )

        df_ty = (
            dataset
            .withColumn('cat_type_dre',
            f.when(((f.col('cd_conta')=='2.01')&(f.col('ds_conta')=='passivo_circulante')),'type_01')
            .when(((f.col('cd_conta')=='2.01')&(f.col('ds_conta')=='passivos_financeiros_ao_valor_justo_atravesresultado')), 'type_02')
            .when(((f.col('cd_conta')=='2.02.01')&(f.col('ds_conta')=='passivo_exigivel_a_longo_prazo')),'type_03')
            )
            .select('cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_refer', 'dt_fim_exerc', 'dt_year', 'dt_quarter', 'cat_type_dre')
            .filter(f.col('cat_type_dre').isNotNull())
            .dropDuplicates()
        )

        varlist_on = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_refer', 'dt_fim_exerc', 'dt_year', 'dt_quarter']
        dataset = (
            dataset
            .join(df_ty, on=varlist_on, how='left')
        )

        # Type 01
        dataset = (
            dataset
            .withColumn('amt_total_liabilities', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='2'), f.col('vl_conta')))
            .withColumn('amt_current_liabilities', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='2.01'), f.col('vl_conta')))
            .withColumn('amt_non_current_liabilities', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='2.02'), f.col('vl_conta')))
            .withColumn('amt_loans_financing', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='2.02.01'), f.col('vl_conta')))
            .withColumn('amt_net_equity', f.when((f.col('cat_type_dre')=='type_01')&(f.col('cd_conta')=='2.03'), f.col('vl_conta')))
        )

        # Type 02
        dataset = (
            dataset
            .withColumn('amt_total_liabilities', f.when((f.col('cat_type_dre')=='type_02')&(f.col('cd_conta')=='2'), f.col('vl_conta')).otherwise(f.col('amt_total_liabilities')))
            .withColumn('amt_provisions', f.when((f.col('cat_type_dre')=='type_02')&(f.col('cd_conta')=='2.03'), f.col('vl_conta')))
            .withColumn('amt_net_equity', f.when((f.col('cat_type_dre')=='type_02')&(f.col('cd_conta')=='2.07'), f.col('vl_conta')).otherwise(f.col('amt_net_equity')))
        )

        # Type 03
        dataset = (
            dataset
            .withColumn('amt_total_liabilities', f.when((f.col('cat_type_dre')=='type_03')&(f.col('cd_conta')=='2'), f.col('vl_conta')).otherwise(f.col('amt_total_liabilities')))
            .withColumn('amt_current_liabilities', f.when((f.col('cat_type_dre')=='type_03')&(f.col('cd_conta')=='2.01'), f.col('vl_conta')).otherwise(f.col('amt_current_liabilities')))
            .withColumn('amt_non_current_liabilities', f.when((f.col('cat_type_dre')=='type_03')&(f.col('cd_conta')=='2.02'), f.col('vl_conta')).otherwise(f.col('amt_non_current_liabilities')))
            .withColumn('amt_long_term_liabilities', f.when((f.col('cat_type_dre')=='type_03')&(f.col('cd_conta')=='2.02.01'), f.col('vl_conta')))
            .withColumn('amt_net_equity', f.when((f.col('cat_type_dre')=='type_03')&(f.col('cd_conta')=='2.03'), f.col('vl_conta')).otherwise(f.col('amt_net_equity')))
        )

        # Rename all variables to upercase
        variablesRename = [['cd_cvm', 'id_cvm'], ['cnpj_cia', 'id_cnpj'], ['codigo_cvm', 'id_cvm'], ['denom_cia','txt_company_name']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])
        # Standardizing id_cnpj
        dataset = dataset.withColumn('id_cnpj', f.regexp_replace(f.col('id_cnpj'), '[./-]', ''))
        dataset = (
            dataset
            .select(*varlist_bpp)
            .groupBy(varlist_bpp[0:8])
            .agg(
                *[f.sum(c).alias(c) for c in varlist_bpp[8:]]
            )
        )

        return dataset


    def _pre_processed_dfc_dfp(self, dataset:DataFrame) -> DataFrame:


        from unidecode import unidecode
        def remove_accent(text):
            return unidecode(text)


        for var in dataset.columns:
            dataset = dataset.withColumnRenamed(var, var.lower())
        ## Getting year and quarter
        for v in ['dt_fim_exerc']:
            dataset = dataset.withColumn(v, f.to_date(f.col(v), 'yyyy-MM-dd'))
        dataset = (
            dataset
            .filter(f.col('ordem_exerc') == 'ÚLTIMO')
            .withColumn('dt_year', f.year('dt_fim_exerc'))
            .withColumn('dt_quarter', f.quarter('dt_fim_exerc'))
            .withColumn('vl_conta', f.when(f.col('escala_moeda') == 'MIL', f.col('vl_conta')*1000).otherwise(f.col('vl_conta')))
        )

        # Rename all variables to upercase
        variablesRename = [['cd_cvm', 'id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name'],
                        ['cd_conta', 'id_account'], ['ds_conta','txt_account'], ['vl_conta', 'amt_account']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])

        dataset = (
                dataset
                .withColumn('id_cnpj', f.regexp_replace(f.col('id_cnpj'), '[./-]', ''))
                .select('id_cvm', 'id_cnpj', 'txt_company_name', 'dt_fim_exerc', 'dt_year', 'dt_quarter', 'id_account', 'txt_account', 'amt_account')
                .withColumn('amt_account', f.col('amt_account').cast('float'))
        )


        remove_accent_udf = f.udf(remove_accent, StringType())
        dataset = (
            dataset
            .fillna(subset=['txt_account'], value='')
            .withColumn('txt_account', remove_accent_udf(f.col('txt_account')))
            .withColumn('txt_account', f.lower(f.col('txt_account')))
            .withColumn('amt_net_cash_operating_activities', f.when(f.col('id_account').rlike('^6.01$'), f.col('amt_account')))
            .withColumn('amt_depreciation_amortization', f.when((f.col('txt_account').rlike('depreciacao|depreciacoes')), f.col('amt_account')))
            .withColumn('amt_net_cash_investment_activities', f.when(f.col('id_account').rlike('^6.02$'), f.col('amt_account')))
            .withColumn('amt_net_cash_financing_activities', f.when(f.col('id_account').rlike('^6.03$'), f.col('amt_account')))
            .withColumn('amt_exchange_variation_without_cash_equivalents', f.when(f.col('id_account').rlike('^6.04$'), f.col('amt_account')))
            .withColumn('amt_increase_in_cash_equivalents', f.when(f.col('id_account').rlike('^6.05$'), f.col('amt_account')))
            .withColumn('amt_final_balance_cash_equivalents', f.when(f.col('id_account').rlike('^6.05.02$'), f.col('amt_account')))
        )

        varslit_analytics = ['amt_net_cash_operating_activities', 'amt_depreciation_amortization', 'amt_net_cash_investment_activities',
                            'amt_net_cash_financing_activities', 'amt_exchange_variation_without_cash_equivalents',
                            'amt_increase_in_cash_equivalents', 'amt_final_balance_cash_equivalents']
        dataset = (
            dataset
            .groupBy('id_cvm', 'id_cnpj', 'txt_company_name', 'dt_fim_exerc', 'dt_year', 'dt_quarter')
            .agg(
                *[f.sum(c).alias(c) for c in varslit_analytics]
            )
        )

        return dataset


    def _pre_processing_dfc_itr(self, df_itr:DataFrame, df_fpd:DataFrame, dataType:str) -> DataFrame:

        from unidecode import unidecode

        def remove_accent(text):
            return unidecode(text)


        def analytics_quarter(dataset, quarter):

            df_quarter = (
                dataset
                .withColumn('amt_net_cash_operating_activities', f.when(f.col('id_account').rlike('^6.01$'), f.col(quarter)))
                .withColumn('amt_depreciation_amortization', f.when((f.col('txt_account').rlike('depreciacao|depreciacoes')), f.col(quarter)))
                .withColumn('amt_net_cash_investment_activities', f.when(f.col('id_account').rlike('^6.02$'), f.col(quarter)))
                .withColumn('amt_net_cash_financing_activities', f.when(f.col('id_account').rlike('^6.03$'), f.col(quarter)))
                .withColumn('amt_exchange_variation_without_cash_equivalents', f.when(f.col('id_account').rlike('^6.04$'), f.col(quarter)))
                .withColumn('amt_increase_in_cash_equivalents', f.when(f.col('id_account').rlike('^6.05$'), f.col(quarter)))
                .withColumn('amt_final_balance_cash_equivalents', f.when(f.col('id_account').rlike('^6.05.02$'), f.col(quarter)))
            )

            varslit_analytics = ['amt_net_cash_operating_activities', 'amt_depreciation_amortization', 'amt_net_cash_investment_activities',
                                'amt_net_cash_financing_activities', 'amt_exchange_variation_without_cash_equivalents',
                                'amt_increase_in_cash_equivalents', 'amt_final_balance_cash_equivalents']
            df_quarter = (
                df_quarter
                .withColumn('dt_quarter', f.lit(quarter[1]).cast('int'))
                .groupBy('id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'dt_quarter')
                .agg(
                    *[f.sum(c).alias(c) for c in varslit_analytics]
                )
            )

            return df_quarter


        dataset = (
            df_itr.union(df_fpd.select(df_itr.columns))
        )

        for var in dataset.columns:
            dataset = dataset.withColumnRenamed(var, var.lower())

        ## Getting year and quarter
        for v in ['dt_refer', 'dt_fim_exerc', 'dt_ini_exerc']:
            dataset = dataset.withColumn(v, f.to_date(f.col(v), 'yyyy-MM-dd'))

        dataset = (
            dataset
            .filter(f.col('ordem_exerc') == 'ÚLTIMO')
            .withColumn('dt_year', f.year('dt_fim_exerc'))
            .withColumn('dt_quarter', f.quarter('dt_fim_exerc'))
            .withColumn('vl_conta', f.when(f.col('escala_moeda') == 'MIL', f.col('vl_conta')*1000).otherwise(f.col('vl_conta')))
        )

        # Rename all variables to upercase
        variablesRename = [['cd_cvm', 'id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name'],
                        ['cd_conta', 'id_account'], ['ds_conta','txt_account'], ['vl_conta', 'amt_account']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])


        dataset = (
            dataset
            .withColumn('id_cnpj', f.regexp_replace(f.col('id_cnpj'), '[./-]', ''))
            .select('id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'dt_quarter',
                    'dt_refer', 'dt_ini_exerc', 'dt_fim_exerc', 'id_account', 'txt_account', 'amt_account')
        )

        df_1 = (
            dataset
            .filter(f.col('dt_quarter')==1)
            .withColumnRenamed('amt_account', 'Q1')
            .drop('dt_refer', 'dt_fim_exerc', 'dt_fim_exerc', 'dt_quarter')
        )

        df_2 = (
            dataset
            .filter(f.col('dt_quarter')==2)
            .withColumnRenamed('amt_account', 'Q2')
            .drop('dt_refer', 'dt_ini_exerc', 'dt_fim_exerc', 'dt_quarter')
        )

        df_3 = (
            dataset
            .filter(f.col('dt_quarter')==3)
            .withColumnRenamed('amt_account', 'Q3')
            .drop('dt_refer', 'dt_ini_exerc', 'dt_fim_exerc', 'dt_quarter')
        )

        df_4 = (
            dataset
            .filter(f.col('dt_quarter')==4)
            .withColumnRenamed('amt_account', 'Q4')
            .drop('dt_refer', 'dt_ini_exerc', 'dt_fim_exerc', 'dt_quarter')
        )

        on_list = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'id_account', 'txt_account']
        df = (
            df_1
            .join(df_2, on=on_list, how='left')
            .join(df_3, on=on_list, how='left')
            .join(df_4, on=on_list, how='left')
            .orderBy('id_cnpj', 'id_account')
        )

        remove_accent_udf = f.udf(remove_accent, StringType())
        df = (
            df
            .fillna(subset=['txt_account'], value='')
            .withColumn('txt_account', f.lower(f.col('txt_account')))
            .withColumn('txt_account', remove_accent_udf(f.col('txt_account')))
            .withColumn('txt_account', f.regexp_replace(f.col('txt_account'), '[,.\\\(\)\/-/&]', ''))
            .withColumn('txt_account', f.regexp_replace(f.col('txt_account'), '\s+e\s+', ' '))
            .withColumn('txt_account', f.regexp_replace(f.col('txt_account'), '\s+de\s+', ' '))
            .withColumn('txt_account', f.regexp_replace(f.col('txt_account'), '\s+do\s+', ' '))
            .withColumn('txt_account', f.regexp_replace(f.col('txt_account'), '\s+dos\s+', ' '))
            .withColumn('txt_account', f.regexp_replace(f.col('txt_account'), '\s+da\s+', ' '))
            .withColumn('txt_account', f.regexp_replace(f.col('txt_account'), '\s+das\s+', ' '))
            .withColumn('txt_account', f.regexp_replace(f.col('txt_account'), '\s+por\s+', ' '))
            .withColumn('txt_account', f.regexp_replace(f.col('txt_account'), '\s+', '_'))
            .withColumn('txt_account', f.lower(f.col('txt_account')))
            .withColumnRenamed('Q2', 'Q2_cumulative')
            .withColumnRenamed('Q3', 'Q3_cumulative')
            .withColumnRenamed('Q4', 'Q4_cumulative')
            .withColumn('Q2', f.col('Q2_cumulative')-f.col('Q1'))
            .withColumn('Q3', f.col('Q3_cumulative')-f.col('Q2_cumulative'))
            .withColumn('Q4', f.col('Q4_cumulative')-f.col('Q3_cumulative'))
            .select('id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'id_account', 'txt_account', 'Q1', 'Q2', 'Q2_cumulative', 'Q3', 'Q3_cumulative', 'Q4', 'Q4_cumulative')
        )

        if dataType == 'itr_dfc':

            dq_1 = analytics_quarter(dataset=df, quarter='Q1')
            dq_1 = (
                dq_1
                .withColumn('dt_fim_exerc', f.lit('2022-03-31'))
                .withColumn('dt_fim_exerc', f.to_date(f.col('dt_fim_exerc'), 'yyyy-MM-dd'))
            )
            dq_2 = analytics_quarter(dataset=df, quarter='Q2')
            dq_2 = (
                dq_2
                .withColumn('dt_fim_exerc', f.lit('2022-06-30'))
                .withColumn('dt_fim_exerc', f.to_date(f.col('dt_fim_exerc'), 'yyyy-MM-dd'))
            )
            dq_3 = analytics_quarter(dataset=df, quarter='Q3')
            dq_3 = (
                dq_3
                .withColumn('dt_fim_exerc', f.lit('2022-09-30'))
                .withColumn('dt_fim_exerc', f.to_date(f.col('dt_fim_exerc'), 'yyyy-MM-dd'))
            )
            dq_4 = analytics_quarter(dataset=df, quarter='Q4')
            dq_4 = (
                dq_4
                .withColumn('dt_fim_exerc', f.lit('2022-12-31'))
                .withColumn('dt_fim_exerc', f.to_date(f.col('dt_fim_exerc'), 'yyyy-MM-dd'))
            )

            df_analytical = (
                dq_1
                .union(dq_2)
                .union(dq_3)
                .union(dq_4)
            )
            varlist = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_fim_exerc', 'dt_year', 'dt_quarter', 'amt_net_cash_operating_activities',
            'amt_depreciation_amortization', 'amt_net_cash_investment_activities', 'amt_net_cash_financing_activities',
            'amt_exchange_variation_without_cash_equivalents', 'amt_increase_in_cash_equivalents', 'amt_final_balance_cash_equivalents']
            
            df_analytical = df_analytical.select(varlist)

            return df_analytical


        elif dataType == 'itr_dfc_table':

            df_table = (
                df
                .select('id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'id_account', 'txt_account', 'Q1', 'Q2', 'Q3', 'Q4')
            )
            return df_table


    def pre_process_cvm(self, dataType:str, schema:StructField, year:str, execution_date:DateType):


        from sparkDocuments import types_dict, DIR_PATH_RAW_FCA, DIR_PATH_RAW_DFP, DIR_PATH_RAW_ITR, DIR_PATH_PROCESSED_FCA_GENERAL_REGISTER, DIR_PATH_PROCESSED_FCA_STOCK_TYPE, DIR_PATH_PROCESSED_DFP, DIR_PATH_PROCESSED_ITR


        def _saving_pre_processing(dataset, path, dataType, file):
                #.partitionBy('id_cvm')
            saveFilename = f'pp_{dataType}_{file[-8:-4]}.parquet'
            dataset.write.format('parquet') \
                .mode('overwrite') \
                .save(os.path.join(path, saveFilename)) 

        def _processed_date(dataset, execution_date):
            dataset = (
             dataset
             .withColumn('processed_at', lit(execution_date))
             .withColumn('processed_at', to_date(col('processed_at'), 'yyyy-MM-dd'))
            )
       
            return dataset

        if dataType == 'fca_aberta_geral':
            list_files = [file for file in os.listdir(DIR_PATH_RAW_FCA) if (file.endswith('.csv')) and re.findall('fca_cia_aberta_geral', file)]
        elif dataType == 'fca_valor_mobiliario':
            list_files = [file for file in os.listdir(DIR_PATH_RAW_FCA) if (file.endswith('.csv')) and re.findall('fca_cia_aberta_valor_mobiliario', file)]
        elif dataType == 'itr_dre' or dataType == 'itr_bpp' or dataType == 'itr_bpa':
            list_files = [file for file in os.listdir(DIR_PATH_RAW_ITR) if (file.endswith('.csv')) and re.findall(types_dict[dataType], file)]
        elif dataType == 'dfp_dre' or dataType == 'dfp_bpp' or dataType == 'dfp_bpa':
            list_files = [file for file in os.listdir(DIR_PATH_RAW_DFP) if (file.endswith('.csv')) and re.findall(types_dict[dataType], file)]
        elif dataType == 'itr_dfc' or dataType == 'itr_dfc_table':
            list_files = [file for file in os.listdir(DIR_PATH_RAW_ITR) if (file.endswith('.csv')) and re.findall('itr_cia_aberta_DFC_MI_con', file)]
        elif dataType == 'dfp_dfc':
            list_files = [file for file in os.listdir(DIR_PATH_RAW_DFP) if (file.endswith('.csv')) and re.findall('dfp_cia_aberta_DFC_MI_con', file)]

        for file in list_files:
            if year == file[-8:-4]:
                # Oppen Datasets
                if dataType == 'fca_aberta_geral':
                    dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_FCA, file), header = True, sep=';', encoding='ISO-8859-1')
                    dataset = self._pre_processing_fca_aberta_geral(dataset = dataset)
                    dataset = _processed_date(dataset=dataset, execution_date=execution_date)
                    _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_FCA_GENERAL_REGISTER)
                elif dataType == 'fca_valor_mobiliario':
                    dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_FCA, file), header = True, sep=';', encoding='ISO-8859-1')
                    dataset = self._pre_processing_fca_valor_mobiliario(dataset = dataset)
                    dataset = _processed_date(dataset=dataset, execution_date=execution_date)
                    _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_FCA_STOCK_TYPE)                
                elif (dataType == 'dfp_dre') or (dataType == 'itr_dre'):
                    if dataType == 'dfp_dre':
                        dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_DFP, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                        dataset, dataset_ds_conta = self._pre_processing_dre(dataset = dataset, type='dfp_dre')
                        dataset = _processed_date(dataset=dataset, execution_date=execution_date)
                        dataset_ds_conta = _processed_date(dataset=dataset_ds_conta, execution_date=execution_date)
                    elif dataType == 'itr_dre':
                        dataset_itr = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_ITR, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                        dataset_dfp = self.spark_environment.read.parquet(os.path.join(DIR_PATH_PROCESSED_DFP, f'pp_dfp_dre_{file[-8:-4]}.parquet'))
                        dataset, dataset_ds_conta = self._pre_processing_dre(dataset = dataset_itr, type='itr_dre')
                        dataset_complete = self._get_last_quarter(df_itr=dataset, df_dfp=dataset_dfp)
                        dataset_complete = _processed_date(dataset=dataset_complete, execution_date=execution_date)
                        dataset = _processed_date(dataset=dataset, execution_date=execution_date)
                        dataset_ds_conta = _processed_date(dataset=dataset_ds_conta, execution_date=execution_date)
                    if dataType == 'dfp_dre':
                        _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_DFP)
                        dataType_ds_conta = 'dfp_dre_ds_conta'
                        _saving_pre_processing(dataset=dataset_ds_conta, dataType=dataType_ds_conta, file=file, path=DIR_PATH_PROCESSED_DFP)
                    elif dataType == 'itr_dre':
                        _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_ITR)
                        complete_dataType = 'itr_dre_complete'
                        _saving_pre_processing(dataset=dataset_complete, dataType=complete_dataType, file=file, path=DIR_PATH_PROCESSED_ITR)
                        complete_dataType = 'itr_dre_ds_conta'
                        _saving_pre_processing(dataset=dataset_ds_conta, dataType=complete_dataType, file=file, path=DIR_PATH_PROCESSED_ITR)
                elif (dataType == 'dfp_bpp') or (dataType == 'itr_bpp'):
                    if dataType == 'dfp_bpp':
                        dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_DFP, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    elif dataType == 'itr_bpp':
                        dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_ITR, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    dataset = self._pre_processing_bpp(dataset = dataset)
                    dataset = _processed_date(dataset=dataset, execution_date=execution_date)
                    if dataType == 'dfp_bpp':
                        _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_DFP)
                    elif dataType == 'itr_bpp':
                        _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_ITR)
                elif (dataType == 'dfp_bpa') or (dataType == 'itr_bpa'):
                    if dataType == 'dfp_bpa':
                        dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_DFP, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    elif dataType == 'itr_bpa':
                        dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_ITR, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    dataset = self._pre_processing_bpa(dataset = dataset)
                    dataset = _processed_date(dataset=dataset, execution_date=execution_date)
                    if dataType == 'dfp_bpa':
                        _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_DFP)
                    elif dataType == 'itr_bpa':
                        _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_ITR)
                elif (dataType == 'itr_dfc') or (dataType == 'itr_dfc_table'):
                    df_itr = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_ITR, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    df_fpd = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_DFP, f'dfp_cia_aberta_DFC_MI_con_{file[-8:-4]}.csv'),header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    if dataType == 'itr_dfc':
                        df_analytical = self._pre_processing_dfc_itr(df_itr=df_itr, df_fpd=df_fpd, dataType='itr_dfc')
                        df_analytical = _processed_date(dataset=df_analytical, execution_date=execution_date)
                        _saving_pre_processing(dataset=df_analytical, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_ITR)
                    elif dataType == 'itr_dfc_table':
                        df_table = self._pre_processing_dfc_itr(df_itr=df_itr, df_fpd=df_fpd, dataType='itr_dfc_table')
                        df_table = _processed_date(dataset=df_table, execution_date=execution_date)
                        _saving_pre_processing(dataset=df_table, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_ITR)
                elif (dataType == 'dfp_dfc'):
                    df_fpd = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_DFP, f'dfp_cia_aberta_DFC_MI_con_{file[-8:-4]}.csv'),header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    df_fpd.show()
                    dataset = self._pre_processed_dfc_dfp(dataset=df_fpd)
                    dataset.show()
                    _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_DFP)


    def get_lag(self, dataset:DataFrame, partition_var:list, order_var:list, type:str, measure:str, variable:str, nlag:int, period:int=None, period_text:str=None) -> DataFrame:

        windowSpec  = Window.partitionBy(partition_var).orderBy(order_var)

        if period_text is not None:
            dataset = dataset.withColumn(f'{type}_{measure}_{variable}_{period_text}_lag',lag(variable, nlag).over(windowSpec))
        else:
            dataset = dataset.withColumn(f'{type}_{measure}_{variable}_{nlag}{period}_lag',lag(variable, nlag).over(windowSpec))

        return dataset


    def get_percentage_change(self, dataset:DataFrame, partition_var:list, order_var:list, variable:str, nlag:int, period:int=None, period_text:str=None):

        windowSpec  = Window.partitionBy(partition_var).orderBy(order_var)
    
        if period_text is not None:
            dataset = (
                dataset
                .withColumn('var_lag',lag(variable, nlag).over(windowSpec))
                .withColumn(f'pct_tx_{variable}_{period_text}', (col(variable)-col('var_lag'))/abs(col('var_lag')))
                .drop('var_lag')
            )       
        else:
            dataset = (
                dataset
                .withColumn('var_lag',lag(variable, nlag).over(windowSpec))
                .withColumn(f'pct_tx_{variable}_{nlag}{period}', (col(variable)-col('var_lag'))/abs(col('var_lag')))
                .drop('var_lag')
            )

        return dataset


    def measure_run(self, dataset:DataFrame, partition_var:list, order_var:list, variable:str, n_periods:int, measure_type:str) -> DataFrame:

        WindowSpec = Window.partitionBy(partition_var).orderBy(order_var).rowsBetween(-n_periods, Window.currentRow)

        if measure_type == 'mean':
            variable_name = f'amt_avg_{variable}_{n_periods}'
            dataset = dataset.withColumn(variable_name, mean(col(variable)).over(WindowSpec))
        elif measure_type == 'std':
            variable_name = f'amt_std_{variable}_{n_periods}'
            dataset = dataset.withColumn(variable_name, stddev(col(variable)).over(WindowSpec))
        elif measure_type == 'var':
            variable_name = f'amt_var_{variable}_{n_periods}'
            dataset = dataset.withColumn(variable_name, variance(col(variable)).over(WindowSpec))
        elif measure_type == 'min':
            variable_name = f'amt_min_{variable}_{n_periods}'
            dataset = dataset.withColumn(variable_name, min(col(variable)).over(WindowSpec))
        elif measure_type == 'max':
            variable_name = f'amt_max_{variable}_{n_periods}'
            dataset = dataset.withColumn(variable_name, max(col(variable)).over(WindowSpec))

        return dataset


    def measure_beta(self, dataset:DataFrame, partition_var:list, order_var:list, variable:str, n_periods:int) -> DataFrame:

        windowSpec1  = Window.partitionBy(partition_var).orderBy(order_var)
        windowSpec2 = Window.partitionBy(partition_var).orderBy(order_var).rowsBetween(-n_periods, Window.currentRow)

        variable_name = f'amt_beta_{variable}_{n_periods+1}'

        dataset = (
            dataset
            .withColumn('y', lag(variable, 1).over(windowSpec1))
            .withColumn('y_mean', mean(col('y')).over(windowSpec2))
            .withColumn('y_y_mean', col(variable)-col('y_mean'))
            .withColumn('x_mean', mean(col(variable)).over(windowSpec2))
            .withColumn('x_x_mean', col(variable)-col('x_mean'))
            .withColumn('num', col('x_x_mean')*col('y_y_mean'))
            .withColumn('den', col('x_x_mean')*col('x_x_mean'))
            .withColumn(variable_name, col('num')/col('den'))
            .drop('y', 'y_mean', 'y_y_mean', 'x_mean', 'x_x_mean', 'num', 'den')
        )

        return dataset


    def broken_stock_high(self, dataset:DataFrame, variable:str, partition_variables:list, order_variables:list, n_periods:int) -> DataFrame:

        WindowSpec = Window.partitionBy(partition_variables).orderBy(order_variables).rowsBetween(-n_periods, -1)

        dataset = (
            dataset
            .withColumn('max', max(col(variable)).over(WindowSpec))
            .withColumn(f'cat_{variable}_broke_high_{n_periods}', when(col(variable) > col('max'), 1).otherwise(0))
            .drop('max')
        )

        return dataset


    def broken_stock_low(self, dataset:DataFrame, variable:str, partition_variables:list, order_variables:list, n_periods:int) -> DataFrame:

        WindowSpec = Window.partitionBy(partition_variables).orderBy(order_variables).rowsBetween(-n_periods, -1)

        dataset = (
            dataset
            .withColumn('min', min(col(variable)).over(WindowSpec))
            .withColumn(f'cat_{variable}_broke_low_{n_periods}', when(col(variable) > col('min'), 1).otherwise(0))
            .drop('min')
        )

        return dataset

    def IFR(self, dataset:DataFrame, partition_var:list, order_var:list, n_periods:int, variable:str):

        dataset = self.get_percentage_change(dataset=dataset, partition_var=partition_var, order_var=order_var, variable=variable, nlag=1, period='d')
   
        WindowSpec = Window.partitionBy(partition_var).orderBy(order_var).rowsBetween(start=-n_periods, end=-1)
        dataset = (
            dataset
            .withColumnRenamed(f'pct_tx_{variable}_1d','pct')
            .withColumn('high_price_mean', when(col('pct')>0, mean(col(variable)).over(WindowSpec)))
            .withColumn('low_price_mean', when(col('pct')<0, mean(col(variable)).over(WindowSpec)))
            .fillna(subset=['high_price_mean', 'low_price_mean'], value=0)
            .withColumn('high_price_mean', sum(col('high_price_mean')).over(WindowSpec))
            .withColumn('low_price_mean', sum(col('low_price_mean')).over(WindowSpec))
            .withColumn(f'amt_IFR_{variable}_{n_periods}', (100-(100/(1+(col('high_price_mean')/col('low_price_mean'))))))
            .drop('pct', 'high_price_mean', 'low_price_mean')
        )

        return dataset