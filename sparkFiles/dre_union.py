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
    from sparkDocuments import schema_pp_dre, DIR_S3_RAW_DFP,  DIR_S3_RAW_ITR, DIR_S3_ANALYTICAL_DRE
    from PreProcessing import PreProcessing
  
    sk = SparkSession.builder.getOrCreate()

    # Append _union_quarters
    pp = PreProcessing(spark_environment=sk)

    dataset = sk.createDataFrame(data=sk.sparkContext.emptyRDD(),schema=schema_pp_dre)

    print('open dataset')
    files_list = __files_list()       
    for file in files_list:
        dataset_itr = sk.read.parquet(os.path.join(DIR_S3_RAW_ITR, file[0]))
        dataset_dfp = sk.read.parquet(os.path.join(DIR_S3_RAW_DFP, file[1]))
        df = pp._union_quarters(dataset_itr=dataset_itr, dataset_dfp=dataset_dfp)
        dataset = dataset.union(df)

    # Saving
    print('saving')
    dataset.write.format('parquet') \
        .mode('overwrite') \
       .save(os.path.join(DIR_S3_ANALYTICAL_DRE, 'pp_dre_union.parquet'))  
 

if __name__ == "__main__":

    _pp_union_dre()

    