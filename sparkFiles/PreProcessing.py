import os
import re
from datetime import date
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, udf
from pyspark.sql.types import StructField, StructType, DateType, DoubleType, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, quarter, to_date, month, year, when, to_date, asc, months_between, round, concat, lit, regexp_replace, sum, max, lag, collect_list, slice, row_number
from pyspark.sql.window import Window


class PreProcessing():


    def __init__(self, spark_environment):
        self._run()
        self.spark_environment = spark_environment


    def _run(self):

        self.todaystr = re.sub('-', '_', str((date.today())))


    def remove_missing_values(self, dataset:DataFrame) -> DataFrame:

        missing_variables = {col: dataset.filter(dataset[col].isNull()).count() for col in dataset.columns}

        return missing_variables


    def text_normalization(row):

        return row.encode('ascii', 'ignore').decode('ascii')


    def patterning_text_var(self, dataset:DataFrame, variable:str) -> DataFrame:

        dataset[variable] = dataset[variable].str.lower().str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        dataset[variable] = dataset[variable].replace(r'\s+', '_', regex=True)

        return dataset


    def _union_quarters(self, dataset_itr:DataFrame, dataset_dfp:DataFrame) -> DataFrame:

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


    def _pre_processing_itr_dre(self, dataset:DataFrame) -> DataFrame:
    
        from sparkDocuments import dre_account

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
        
        from sparkDocuments import bpp_account

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

        from sparkDocuments import bpa_account

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

        from sparkDocuments import dre_account

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


    def _pre_processing_dfp_bpa(self, dataset:DataFrame) -> DataFrame:

        from sparkDocuments import bpa_account

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
        ## Standarting escala_moeda
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


    def _pre_processing_dfp_bpp(self, dataset:DataFrame) -> DataFrame:

        from sparkDocuments import bpp_account

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
        ## Standarting escala_moeda
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


    def pre_process_cvm(self, dataType:str, schema:StructField, year:str, execution_date:DateType):


        from sparkDocuments import types_dict, DIR_PATH_RAW_DFP, DIR_PATH_RAW_ITR, DIR_PATH_PROCESSED_DFP, DIR_PATH_PROCESSED_ITR


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


        if dataType == 'itr_dre' or dataType == 'itr_bpp' or dataType == 'itr_bpa':
            list_files = [file for file in os.listdir(DIR_PATH_RAW_ITR) if (file.endswith('.csv')) and re.findall(types_dict[dataType], file)]
        elif dataType == 'dfp_dre' or dataType == 'dfp_bpp' or dataType == 'dfp_bpa':
            list_files = [file for file in os.listdir(DIR_PATH_RAW_DFP) if (file.endswith('.csv')) and re.findall(types_dict[dataType], file)]
    

        for file in list_files:
            if year == file[-8:-4]:
                # Oppen Datasets
                if dataType == 'itr_dre':
                    dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_ITR, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    dataset = self._pre_processing_itr_dre(dataset = dataset)
                    dataset = _processed_date(dataset=dataset, execution_date=execution_date)
                    _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_ITR)
                elif dataType == 'itr_bpp':
                    dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_ITR, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    dataset = self._pre_processing_itr_bpp(dataset = dataset)
                    dataset = _processed_date(dataset=dataset, execution_date=execution_date)                   
                    _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_ITR)
                elif dataType == 'itr_bpa':
                    dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_ITR, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    dataset = self._pre_processing_itr_bpa(dataset = dataset)
                    dataset = _processed_date(dataset=dataset, execution_date=execution_date)        
                    _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_ITR)
                elif dataType == 'dfp_dre':
                    dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_DFP, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    dataset = self._pre_processing_dfp_dre(dataset = dataset)
                    dataset = _processed_date(dataset=dataset, execution_date=execution_date)
                    _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_DFP)
                elif dataType == 'dfp_bpp':
                    dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_DFP, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    dataset = self._pre_processing_dfp_bpp(dataset = dataset)
                    dataset = _processed_date(dataset=dataset, execution_date=execution_date)
                    _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_DFP)
                elif dataType == 'dfp_bpa':
                    dataset = self.spark_environment.read.csv(os.path.join(DIR_PATH_RAW_DFP, file), header = True, sep=';', encoding='ISO-8859-1', schema=schema)
                    dataset = self._pre_processing_dfp_bpa(dataset = dataset)
                    dataset = _processed_date(dataset=dataset, execution_date=execution_date)
                    _saving_pre_processing(dataset=dataset, dataType=dataType, file=file, path=DIR_PATH_PROCESSED_DFP)


    def get_lag(self, dataset:DataFrame, type:str, measure:str, variable:str, nlag:int, period:int=None, period_text:str=None) -> DataFrame:

        windowSpec  = Window.partitionBy('id_cvm').orderBy(['dt_year','dt_quarter'])

        if period_text is not None:
            dataset = dataset.withColumn(f'{type}_{measure}_{variable}_{period_text}_lag',lag(variable, nlag).over(windowSpec))
        else:
            dataset = dataset.withColumn(f'{type}_{measure}_{variable}_{nlag}{period}_lag',lag(variable, nlag).over(windowSpec))

        return dataset


    def get_percentage_change(self, dataset:DataFrame, variable:str, nlag:int, period:int=None, period_text:str=None):

        windowSpec  = Window.partitionBy('id_cvm').orderBy(['dt_year','dt_quarter'])
    
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


    def measure_run(self, dataset:DataFrame, variable:str, n_periods:int, measure_type:str) -> DataFrame:

        import numpy as np

        def _mean_func(x):
            result = np.mean(x)
            return float(result)  

        def _median_func(x):
            result = np.median(x)
            return float(result)   

        def _std_func(x):
            result = np.std(x)
            return float(result)

        def _var_func(x):
            
            result = np.var(x)
            return float(result)

        def _min_func(x):
            
            result = np.min(x)
            return float(result)

        def _max_func(x):
            
            result = np.max(x)
            return float(result)

        
        if measure_type == 'mean':
            variable_name = f'amt_avg_{variable}_{n_periods}'
            func_udf = udf(_mean_func, DoubleType())
        elif measure_type == 'median':
            variable_name = f'amt_mda_{variable}_{n_periods}'
            func_udf = udf(_median_func, DoubleType())
        elif measure_type == 'std':
            variable_name = f'amt_std_{variable}_{n_periods}'
            func_udf = udf(_var_func, DoubleType())
        elif measure_type == 'var':
            variable_name = f'amt_var_{variable}_{n_periods}'
            func_udf = udf(_std_func, DoubleType())
        elif measure_type == 'min':
            variable_name = f'amt_min_{variable}_{n_periods}'
            func_udf = udf(_min_func, DoubleType())
        elif measure_type == 'max':
            variable_name = f'amt_max_{variable}_{n_periods}'
            func_udf = udf(_max_func, DoubleType())

        windowSpec  = Window.partitionBy('id_cvm').orderBy(['dt_year','dt_quarter'])

        df = (
            dataset
            .orderBy(['id_cvm','dt_year','dt_quarter'])
            .groupBy('id_cvm')
            .agg(collect_list(variable).alias('array'))
        )

        dataset = (
            dataset
            .withColumn('row_n', row_number().over(windowSpec))
            .join(df, on='id_cvm', how='left')
            .withColumn('array_slice', slice(col('array'), col('row_n'), n_periods))
            .withColumn(variable_name, func_udf(col('array_slice')))
            .drop(*['array', 'array_slice'])
        )

        return dataset


    def measure_beta(self, dataset, variable, n_periods) -> DataFrame:

        def _beta(x, y, deg=1):
            try:    
                coef = np.polyfit(x, y, deg=deg)
                coef = coef[-1]    
                return float(coef)
            except:
                return float(np.nan)

        
        variable_name = f'amt_beta_{variable}_{n_periods}'
        func_udf = udf(_beta, DoubleType())

        windowSpec  = Window.partitionBy('id_cvm').orderBy(['dt_year','dt_quarter'])

        df = (
            dataset
            .orderBy(['id_cvm','dt_year','dt_quarter'])
            .withColumn('y_array', row_number().over(windowSpec))
            .groupBy('id_cvm')
            .agg(collect_list(variable).alias('x_array'), collect_list('y_array').alias('y_array'))
        )


        dataset = (
            dataset
            .withColumn('row_n', row_number().over(windowSpec))
            .join(df, on='id_cvm', how='left')
            .withColumn('x_array_slice', slice(col('x_array'), col('row_n'), n_periods))
            .withColumn('y_array_slice', slice(col('y_array'), col('row_n'), n_periods))
            .withColumn(variable_name, func_udf(col('x_array_slice'), col('y_array_slice')))
            .drop(*['row_n', 'x_array', 'x_array_slice', 'y_array', 'y_array_slice'])
        )

        return dataset