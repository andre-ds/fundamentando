from datetime import date


def __files_list():
    years_list = [*range(2011, date.today().year+1, 1)]
    files_list = []
    for y in years_list:
        zip_files = [f'pp_itr_dre_{y}.parquet', f'pp_dfp_dre_{y}.parquet']
        files_list.append(zip_files)

    return files_list


def _pp_union_dre():
    
    import os
    from pyspark.sql import SparkSession
    from sparkDocuments import schema_pp_dre, DIR_S3_PRE_PROCESSED_DFP,  DIR_S3_PRE_PROCESSED_ITR, DIR_S3_ANALYTICAL
    from PreProcessing import PreProcessing
    import pyspark.sql.functions as f
    from pyspark.sql.window import Window

    sk = SparkSession.builder.getOrCreate()

    # Append _union_quarters
    pp = PreProcessing(spark_environment=sk)

    dataset = sk.createDataFrame(data=sk.sparkContext.emptyRDD(),schema=schema_pp_dre)

    print('open dataset')
    files_list = __files_list()                
    for file in files_list:
        try:
            dataset_itr = sk.read.parquet(os.path.join(DIR_S3_PRE_PROCESSED_ITR, file[0]))
            dataset_dfp = sk.read.parquet(os.path.join(DIR_S3_PRE_PROCESSED_DFP, file[1]))
            df = pp._union_quarters(dataset_itr=dataset_itr, dataset_dfp=dataset_dfp)
            dataset = dataset.union(df)
        except:
            print(f'The {file} is not available!')   

    # Removing Duplicates Errors
    windowSpec = Window.partitionBy('id_cnpj', 'dt_year', 'dt_quarter').orderBy('dt_fim_exerc')
    dataset = (
        dataset
        .dropDuplicates(subset=['id_cnpj', 'dt_refer', 'dt_ini_exerc','dt_fim_exerc'])
        .withColumn('duplicated', f.count(f.col('id_cnpj')).over(windowSpec))
        .filter(f.col('duplicated')==1)
        .drop('duplicated')
    )

    # Saving
    print('saving')
    dataset.write.format('parquet') \
        .mode('overwrite') \
        .save(os.path.join(DIR_S3_ANALYTICAL, 'pp_dre_union.parquet'))  

if __name__ == "__main__":

    _pp_union_dre()

    