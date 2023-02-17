import argparse


def get_stock_information(ticker_list, reference_date):
    
    import os
    import investpy
    import yfinance as yf
    import pandas as pd
    from pyspark.context import SparkContext
    from pyspark.conf import SparkConf
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f
    from pyspark.sql.types import IntegerType, FloatType
    from sparkDocuments import DIR_PATH_RAW_STOCK, DIR_PATH_PROCESSED_FCA_STOCK_TYPE
    from PreProcessing import PreProcessing

    sk = SparkSession(SparkContext(conf=SparkConf())\
    .getOrCreate())

    pp = PreProcessing(spark_environment=sk)

    print(f'execution_date: {reference_date}')
    start = pp.get_start(execution_date=reference_date)
    end = pp.get_end(execution_date=reference_date)

    # Stocks investpy
    if ticker_list=='API':
        dataset_stocks = investpy.get_stocks(country='brazil')
        dataset_stocks.rename(columns={
            'symbol':'ticker',
            'isin':'id_isin'}, inplace=True)
        dataset_stocks['ticker'] = dataset_stocks['ticker'] + '.SA'
        dataset_stocks = dataset_stocks[['ticker', 'id_isin']]
        ticker_list = dataset_stocks['ticker'].to_list()
    elif ticker_list=='file':
        dtype={
                'id_isin':'object',
                'ticker':'object',
                'id_cnpj':'object'
            }
        dataset_stocks = pd.read_csv(os.path.join(DIR_PATH_PROCESSED_FCA_STOCK_TYPE, 'register_2023_01_27_stock_tickers.csv'), dtype=dtype)
        ticker_list = dataset_stocks['ticker_list'].to_list()
    try:
        dataset = yf.download(ticker_list, start=start, end=end,  actions=True)
        dataset = dataset.stack().reset_index()
        dataset.rename(columns={
            'Date':'date',
            'level_1':'ticker_list',
            'Adj Close':'adj_close',
            'Close':'close',
            'Dividends':'dividends',
            'High':'high',
            'Low':'low',
            'Open':'open',
            'Stock Splits':'stock_splits',
            'Volume':'volume'   
        }, inplace=True)
        dataset['date']=dataset['date'].astype(str)
        dataset = pd.merge(dataset, dataset_stocks, on='ticker_list', how='left')
        print(dataset.columns)
        variables = ['date', 'id_cnpj', 'id_ticker', 'adj_close',
                    'close', 'dividends', 'high', 'low',
                    'open', 'stock_splits', 'volume']
        dataset = dataset[variables]
        if dataset.empty:
            raise Exception
        print('createDataFrame-antes')
        print(dataset.head())
        print(type(dataset))
        dataset = sk.createDataFrame(data=dataset)
        print(type(dataset))
        print(dataset.show())
        print('createDataFrame-dps')
        variables = ['dt_date', 'id_cnpj', 'id_ticker','amt_adj_close', 'amt_close', 'amt_high', 
                    'amt_low', 'amt_open', 'qty_volume', 'amt_dividends', 'cat_stock_splits']
        dataset = (
            dataset
            .filter(f.col('date') == start)
            .withColumn('dt_date', f.to_date(f.col('date'), 'yyyy-MM-dd'))
            .withColumn('amt_adj_close', f.col('adj_close').cast(FloatType()))
            .withColumn('amt_close', f.col('close').cast(FloatType()))
            .withColumn('amt_high', f.col('high').cast(FloatType()))
            .withColumn('amt_low', f.col('low').cast(FloatType()))
            .withColumn('amt_open', f.col('open').cast(FloatType()))
            .withColumn('qty_volume', f.col('volume').cast(FloatType()))
            .withColumn('amt_dividends', f.col('dividends').cast(IntegerType()))
            .withColumn('cat_stock_splits', f.col('stock_splits').cast(IntegerType()))
            .select(variables)
            )
        print(dataset.show())
        # Saving
        reference_date = start.replace('-', '_')
        if dataset.isEmpty() == False:
            dataset.write.format('parquet') \
                .mode('overwrite') \
            .save(os.path.join(DIR_PATH_RAW_STOCK, f'extracted_{reference_date}_stock.parquet'))
                                                               
    except:
            print('No data found, symbol may be delisted')


if __name__ == "__main__":
  
    parser = argparse.ArgumentParser(
        description="Spark Pre-processing"
    )
    parser.add_argument("--ticker_list_type", required=True)
    parser.add_argument("--reference_date", required=True)
    args = parser.parse_args()
    
    print(f'Data de start: {args.reference_date}')
    get_stock_information(ticker_list=args.ticker_list_type ,reference_date=args.reference_date)
