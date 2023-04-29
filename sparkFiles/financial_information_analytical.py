import argparse


def pp_financial_union(type_file):

    import os
    from PreProcessing import PreProcessing
    from sparkDocuments import varlist_financial_information_analytical, DIR_PATH_PROCESSED_STOCK, DIR_PATH_PROCESSED_DFP, DIR_PATH_PROCESSED_ITR, DIR_PATH_ANALYTICAL
    from sparkDocuments import schema_pp_bpa_bpp
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as f

    sk = SparkSession.builder.getOrCreate()
    pp = PreProcessing(spark_environment=sk)

    dataset = sk.createDataFrame(data=sk.sparkContext.emptyRDD(), schema=schema_pp_bpa_bpp)

    if type_file=='dfp_all':
        files_list = pp.files_list(type_file=type_file)
        saveFileName = 'analytical_FPD_financial_information.parquet'
        DIR_PATH = DIR_PATH_PROCESSED_DFP
        on_list = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'processed_at']
    elif type_file=='itr_all':
        files_list = pp.files_list(type_file=type_file)
        saveFileName = 'analytical_ITR_financial_information.parquet'
        DIR_PATH = DIR_PATH_PROCESSED_ITR
        on_list = ['id_cvm', 'id_cnpj', 'txt_company_name', 'dt_year', 'dt_quarter', 'processed_at']

    for file in files_list:
        print(file)
        dataset_dre= sk.read.parquet(os.path.join(DIR_PATH, file[0]))
        dataset_bpa= sk.read.parquet(os.path.join(DIR_PATH, file[1]))
        dataset_bpp= sk.read.parquet(os.path.join(DIR_PATH, file[2]))
        
        df = (
            dataset_dre
            .join(dataset_bpa, on=on_list, how='left')
            .join(dataset_bpp, on=on_list, how='left')
            .select(varlist_financial_information_analytical)
        )
        dataset = dataset.union(df)
    
    dataset = dataset.withColumn('id', f.regexp_replace(f.col('id_cnpj'), '[./-]', ''))
    dataset_stock = sk.read.parquet(os.path.join(DIR_PATH_PROCESSED_STOCK, 'pp_stock_union.parquet'))
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
        .join(dataset, on=['id', 'dt_year', 'dt_quarter'], how='left')
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


