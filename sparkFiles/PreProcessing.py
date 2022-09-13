import os
import re
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField, StructType, DateType, DoubleType, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, quarter, to_date, month, year, when, to_date, asc, months_between, round, concat, lit, regexp_replace, sum, max


class PreProcessing():


    def __init__(self, spark_environment):
        self.spark_environment = spark_environment


    def remove_missing_values(self, dataset:DataFrame) -> DataFrame:

        missing_variables = {col: dataset.filter(dataset[col].isNull()).count() for col in dataset.columns}

        return missing_variables


    def text_normalization(row):

        return row.encode('ascii', 'ignore').decode('ascii')


    def patterning_text_var(self, dataset:DataFrame, variable:str) -> DataFrame:

        dataset[variable] = dataset[variable].str.lower().str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        dataset[variable] = dataset[variable].replace(r'\s+', '_', regex=True)

        return dataset


    def _union_quarters(self, dataset_itr, dataset_dfp):

        # Agregations Q1, Q2 and Q3
        df_sum = dataset_itr.groupBy(['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year']).agg(
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
        dataset_q4 = dataset_q4.groupBy(['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year']).agg(
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


    def _pre_processing_itr_dre(self, dataset:DataFrame) -> DataFrame:
    
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

        # Pre-processing
        for var in dataset.columns:
            dataset = dataset.withColumnRenamed(var, var.lower())
        # Keeping last registers
        dataset = dataset.filter(col('ordem_exerc') == 'ÚLTIMO')
        ## Getting year and quarter
        for v in ['dt_refer', 'dt_fim_exerc', 'dt_ini_exerc']:
            dataset = dataset.withColumn(v, to_date(col(v), 'yyyy-MM-dd'))
        dataset = dataset.withColumn('dt_months_exercise', round(months_between(col('dt_fim_exerc'), col('dt_ini_exerc'))))
        dataset = dataset.withColumn('dt_year', year('dt_refer'))
        dataset = dataset.withColumn('dt_quarter', quarter('dt_refer'))
        dataset = dataset.filter(col('dt_months_exercise') == 3)
        ## Standarting escala_moeda
        dataset = dataset.withColumn('vl_conta', when(col('escala_moeda') == 'MIL', col('vl_conta')*1000).otherwise(col('vl_conta')))
        ## Keeping only relevants cd_conta
        listKey = list(dre_account.keys())
        dataset = dataset.filter(col('cd_conta').isin(listKey))
        ## Standarting CD_CONTA
        for k in listKey:
            dataset = dataset.withColumn('cd_conta', regexp_replace('cd_conta', '^' + k + '$', dre_account.get(k)))
        # Pivot dataset
        varlist = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_refer', 'dt_fim_exerc', 'dt_ini_exerc', 'dt_year', 'dt_quarter']
        dataset = dataset.groupBy(varlist).pivot('cd_conta').max('vl_conta').na.fill(0)
        # Rename all variables to upercase
        variablesRename = [['cd_cvm', 'id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])

        return dataset


    def _pre_processing_itr_bpp(self, dataset:DataFrame) -> DataFrame:
        
        from sparkFiles.sparkDocuments import bpp_account

        # Pre-processing
        for var in dataset.columns:
            dataset = dataset.withColumnRenamed(var, var.lower())
        # Keeping las registers
        dataset = dataset.filter(col('ordem_exerc') == 'ÚLTIMO')
        ## Getting year and quarter
        for v in ['dt_refer', 'dt_fim_exerc']:
            dataset = dataset.withColumn(v, to_date(col(v), 'yyyy-MM-dd'))
        dataset = dataset.withColumn('dt_year', year('dt_refer'))
        dataset = dataset.withColumn('dt_quarter', quarter('dt_refer'))
        ## Standarting ESCALA_MOEDA
        dataset = dataset.withColumn('vl_conta', when(col('escala_moeda') == 'MIL', col('vl_conta')*1000).otherwise(col('vl_conta')))
        ## Keeping only relevants cd_conta
        listKey = list(bpp_account.keys())
        dataset = dataset.filter(col('cd_conta').isin(listKey))
        for k in listKey:
            dataset = dataset.withColumn('cd_conta', regexp_replace('cd_conta', '^' + k + '$', bpp_account.get(k)))
        # Pivot dataset
        varlist = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_year', 'dt_quarter']
        dataset = dataset.groupBy(varlist).pivot('cd_conta').max('vl_conta').na.fill(0)
        # Rename all variables to upercase
        variablesRename = [['cd_cvm','id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])

        return dataset
        

    def _pre_processing_itr_bpa(self, dataset:DataFrame) -> DataFrame:

        from sparkFiles.sparkDocuments import bpa_account

        # Pre-processing
        for var in dataset.columns:
            dataset = dataset.withColumnRenamed(var, var.lower())
        # Keeping las registers
        dataset = dataset.filter(col('ordem_exerc') == 'ÚLTIMO')
        ## Getting year and quarter
        for v in ['dt_refer', 'dt_fim_exerc']:
            dataset = dataset.withColumn(v, to_date(col(v), 'yyyy-MM-dd'))
        dataset = dataset.withColumn('dt_year', year('dt_refer'))
        dataset = dataset.withColumn('dt_quarter', quarter('dt_refer'))
        ## Standarting ESCALA_MOEDA
        dataset = dataset.withColumn('vl_conta', when(col('escala_moeda') == 'MIL', col('vl_conta')*1000).otherwise(col('vl_conta')))
        ## Keeping only relevants cd_conta
        listKey = list(bpa_account.keys())
        dataset = dataset.filter(col('cd_conta').isin(listKey))
        for k in listKey:
            dataset = dataset.withColumn('cd_conta', regexp_replace('cd_conta', '^' + k + '$', bpa_account.get(k)))
        # Pivot dataset
        varlist = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_year', 'dt_quarter']
        dataset = dataset.groupBy(varlist).pivot('cd_conta').max('vl_conta').na.fill(0)
        # Rename all variables to upercase
        variablesRename = [['cd_cvm','id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])

        return dataset


    def _pre_processing_dfp_dre(self, dataset:DataFrame) -> DataFrame:

        from sparkFiles.sparkDocuments import dre_account

        # Pre-processing
        for var in dataset.columns:
            dataset = dataset.withColumnRenamed(var, var.lower())
        # Keeping last registers
        dataset = dataset.filter(col('ordem_exerc') == 'ÚLTIMO')
        ## Getting year and quarter
        for v in ['dt_refer', 'dt_fim_exerc', 'dt_ini_exerc']:
            dataset = dataset.withColumn(v, to_date(col(v), 'yyyy-MM-dd'))
        dataset = dataset.withColumn('dt_months_exercise', round(months_between(col('dt_fim_exerc'), col('dt_ini_exerc'))))
        dataset = dataset.withColumn('dt_year', year('dt_refer'))
        dataset = dataset.withColumn('dt_quarter', quarter('dt_refer'))
        dataset = dataset.filter(col('dt_months_exercise') == 12)
        ## Standarting escala_moeda
        dataset = dataset.withColumn('vl_conta', when(col('escala_moeda') == 'MIL', col('vl_conta')*1000).otherwise(col('vl_conta')))
        ## Keeping only relevants cd_conta
        listKey = list(dre_account.keys())
        dataset = dataset.filter(col('cd_conta').isin(listKey))
        ## Standarting CD_CONTA
        for k in listKey:
            dataset = dataset.withColumn('cd_conta', regexp_replace('cd_conta', '^' + k + '$', dre_account.get(k)))
        # Pivot dataset
        varlist = ['cd_cvm', 'cnpj_cia', 'denom_cia', 'dt_refer', 'dt_fim_exerc', 'dt_ini_exerc', 'dt_year', 'dt_quarter']
        dataset = dataset.groupBy(varlist).pivot('cd_conta').max('vl_conta').na.fill(0)
        # Rename all variables to upercase
        variablesRename = [['cd_cvm', 'id_cvm'], ['cnpj_cia', 'id_cnpj'], ['denom_cia','txt_company_name']]
        for v in variablesRename:
            dataset = dataset.withColumnRenamed(v[0], v[1])

        return dataset


    def pre_process_cvm(self, dataType:str, schema:StructField, year:str):
        
        from sparkFiles.sparkDocuments import types_dict, DIR_PATH_RAW, DIR_PATH_PROCESSED
 

        list_files = [file for file in os.listdir(DIR_PATH_RAW) if (file.endswith('.csv')) and re.findall(types_dict[dataType], file)]
        for file in list_files:
            if year == file[-8:-4]:
                # Oppen Datasets
                dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                if dataType == 'itr_dre':
                    dataset = self._pre_processing_itr_dre(dataset = dataset)
                elif dataType == 'itr_bpp':
                    dataset = self._pre_processing_itr_bpp(dataset = dataset)
                elif dataType == 'itr_bpa':
                    dataset = self._pre_processing_itr_bpa(dataset = dataset)
                elif dataType == 'dfp_dre':
                    dataset = self._pre_processing_dfp_dre(dataset = dataset)
                # Saving
                saveFilename = f'pp_{dataType}_{file[-8:-4]}.parquet'
                dataset.write.format('parquet').mode('overwrite').save(os.path.join(DIR_PATH_PROCESSED, saveFilename))




