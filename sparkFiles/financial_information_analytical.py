import argparse


def pp_financial_union(type_file):

    import os
    from PreProcessing import PreProcessing
    from sparkDocuments import varlist_financial_information_analytical, DIR_PATH_PROCESSED_STOCK, DIR_PATH_PROCESSED_DFP, DIR_PATH_PROCESSED_ITR, DIR_PATH_ANALYTICAL
    from sparkDocuments import schema_financial_information
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as f

    sk = SparkSession.builder.getOrCreate()
    pp = PreProcessing(spark_environment=sk)

    dataset = sk.createDataFrame(data=sk.sparkContext.emptyRDD(), schema=schema_financial_information)

    if type_file=='dfp_all':
        files_list = pp.files_list(type_file=type_file)
        saveFileName = 'analytical_FPD_financial_information.parquet'
        DIR_PATH = DIR_PATH_PROCESSED_DFP
    elif type_file=='itr_all':
        files_list = pp.files_list(type_file=type_file)
        saveFileName = 'analytical_ITR_financial_information.parquet'
        DIR_PATH = DIR_PATH_PROCESSED_ITR

    for file in files_list:
        print(file)
        dataset_dre = (
            sk.read.parquet(os.path.join(DIR_PATH, file[0]))
            .drop('dt_ini_exerc', 'cat_type_dre')
            .withColumn('dt_refer', f.col('dt_refer').cast('string'))
            .withColumn('dt_fim_exerc', f.col('dt_fim_exerc').cast('string'))
        )
        dataset_bpa = (
            sk.read.parquet(os.path.join(DIR_PATH, file[1]))
            .drop('cat_type_dre')
            .withColumn('dt_refer', f.col('dt_refer').cast('string'))
            .withColumn('dt_fim_exerc', f.col('dt_fim_exerc').cast('string'))
        )
        dataset_bpp = (
            sk.read.parquet(os.path.join(DIR_PATH, file[2]))
            .drop('cat_type_dre')
            .withColumn('dt_refer', f.col('dt_refer').cast('string'))
            .withColumn('dt_fim_exerc', f.col('dt_fim_exerc').cast('string'))
        )
        dataset_dfc = (
            sk.read.parquet(os.path.join(DIR_PATH, file[3]))
            .withColumn('dt_fim_exerc', f.col('dt_fim_exerc').cast('string'))
        )
        print('teste-1')
        on_list = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'dt_quarter', 'dt_fim_exerc', 'dt_refer', 'processed_at']
        df = (
            dataset_dre
            .join(dataset_bpa, on=on_list, how='left')
            .join(dataset_bpp, on=on_list, how='left')
            .join(dataset_dfc, on=['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'dt_quarter', 'dt_fim_exerc', 'processed_at'], how='left')
        )
        dataset = dataset.union(df.select(dataset.columns))
    print('teste-2')
    dataset = dataset.withColumn('id', f.regexp_replace(f.col('id_cnpj'), '[./-]', ''))
    dataset_stock = sk.read.parquet(os.path.join(DIR_PATH_PROCESSED_STOCK, 'pp_stock_union.parquet'))
    dataset_stock = (
        dataset_stock
        .withColumn('dt_year', f.year(f.col('dt_date')))
        .withColumn('dt_quarter', f.quarter(f.col('dt_date')))
        .groupBy('id_cnpj', 'id_ticker' ,'dt_year', 'dt_quarter')
        .agg(f.max(f.col('dt_date')).alias('dt_date'))
        .join(dataset_stock, on=['id_cnpj', 'id_ticker', 'dt_date'], how='inner')
        .withColumnRenamed('id_cnpj', 'id')
        .select('id', 'id_ticker','dt_year', 'dt_quarter', 'amt_adj_close', 'amt_dividends')
    )
    print('teste-3')
    dataset = (
        dataset   
        .join(dataset_stock, on=['id', 'dt_year', 'dt_quarter'], how='left')
        .select(varlist_financial_information_analytical)
        )
    print('teste-4')
    if type_file=='itr_all':
        fileFPD = 'analytical_FPD_financial_information.parquet'
        dataset_fpd = sk.read.parquet(os.path.join(DIR_PATH_ANALYTICAL, fileFPD)).drop('processed_at')
        # All Quarters Union
        agg_funcs_1 = [f.sum(x).alias(f"{x}") for x in dataset.columns[9:]]
        df_sum = (
            dataset
            .drop('dt_quarter', 'dt_refer', 'dt_fim_exerc')
            .groupBy('id_cvm', 'id_cnpj', 'id_ticker', 'txt_company_name', 'cat_type_dre', 'dt_year')
            .agg(*agg_funcs_1)
            .withColumn('dt_quarter', f.lit(None))
            .withColumn('dt_refer', f.lit(None))
            .withColumn('dt_fim_exerc', f.lit(None))
        )
        print('teste-5')
        for v in df_sum.columns[6:]:
            df_sum = df_sum.withColumn(v, -f.col(v))

        # Calculating Last Quarter Dataset
        df_quarter = (
            dataset_fpd
            .union(df_sum.select(dataset_fpd.columns))
            .drop('dt_quarter', 'dt_refer', 'dt_fim_exerc')
        )
        print('teste-6')
        agg_func_2 = [f.sum(x).alias(f"{x}") for x in df_quarter.columns[6:]]
        df_quarter = (
            df_quarter
            .groupBy('id_cvm', 'id_cnpj', 'id_ticker', 'txt_company_name', 'dt_year')
            .agg(*agg_func_2)
        )

        df_ident = (
            dataset_fpd
            .select('id_cvm', 'id_cnpj', 'id_ticker', 'txt_company_name', 'dt_refer', 'dt_year', 'dt_quarter', 'dt_fim_exerc')
            .dropDuplicates()
        )

        df_quarter = (
            df_ident
            .join(df_quarter, on=['id_cvm', 'id_cnpj', 'id_ticker', 'txt_company_name', 'dt_year'], how='left')
        )

        # Complete Dataset
        dataset = (
            dataset
            .union(df_quarter.select(dataset.columns))
            .orderBy('id_cnpj', 'dt_year', 'dt_quarter')
        )

    dataset = (
        dataset
        .withColumn('ebit_amplo', f.col('amt_earnings_before_income_tax_and_social_contribution')+f.abs(f.col('amt_financial_expenses')))
        .withColumn('provisao_ircs_ampla', (f.abs(f.col('amt_income_tax_social_contribution_on_profit'))/(f.abs('amt_earnings_before_income_tax_and_social_contribution')+f.abs('amt_equity_equivalence')))*f.col('ebit_amplo')+f.col('amt_equity_equivalence'))
        .withColumn('nopat_amplo', f.col('ebit_amplo')-f.col('provisao_ircs'))
        .withColumn('ebit_restrit', f.col('ebit_amplo')-f.col('amt_financial_income')+f.col('amt_equity_equivalence'))
        .withColumn('provisao_ircs_restrita', (f.abs(f.col('amt_income_tax_social_contribution_on_profit'))/(f.abs('amt_earnings_before_income_tax_and_social_contribution')+f.abs('amt_equity_equivalence')))*f.col('ebit_amplo'))

    )

    # Saving
    print('saving')
    dataset.write.format('parquet') \
        .mode('overwrite') \
        .save(os.path.join(DIR_PATH_ANALYTICAL, saveFileName))  


if __name__ == '__main__':
      
    parser = argparse.ArgumentParser(
        description="Spark Pre-processing"
    )
    parser.add_argument("--type_file", required=True)
    args = parser.parse_args()

    pp_financial_union(type_file=args.type_file)


