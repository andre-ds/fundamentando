import argparse


def get_stock_information(ticker_list, start, end=None):
    
    import os
    from datetime import datetime, timedelta
    import investpy
    import yfinance as yf
    import pandas as pd
    from pyspark.context import SparkContext
    from pyspark.conf import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import to_date, col
    from pyspark.sql.types import IntegerType, FloatType
    from sparkDocuments import PATH_DATALAKE, DIR_PATH_RAW_STOCK
    
    def _get_end(start):

        start = datetime.strptime(start, '%Y-%m-%d').date()
        end = start + timedelta(1)
        end = end.strftime('%Y-%m-%d')
        
        return end

    sk = SparkSession(SparkContext(conf=SparkConf())\
    .getOrCreate())
    
    if end is None:
        end = _get_end(start=start)

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
        dataset_stocks = pd.read_csv(os.path.join(PATH_DATALAKE, 'register_2022-12-13.csv'), dtype=dtype)
        ticker_list = dataset_stocks['ticker'].to_list()
    try:
        dataset = yf.download(ticker_list, start=start, end=end,  actions=True)
        dataset = dataset.stack().reset_index()
        dataset.rename(columns={
            'Date':'date',
            'level_1':'ticker',
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
        dataset = pd.merge(dataset, dataset_stocks, on='ticker', how='left')
        print(dataset.columns)
        variables = ['date', 'id_isin', 'id_cnpj', 'ticker', 'adj_close',
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
        dataset = (
            dataset
            .filter(col('date') == start)
            .withColumn('date', to_date(col('date'), 'yyyy-MM-dd'))
            .withColumn('adj_close', col('adj_close').cast(FloatType()))
            .withColumn('close', col('close').cast(FloatType()))
            .withColumn('high', col('high').cast(FloatType()))
            .withColumn('low', col('low').cast(FloatType()))
            .withColumn('open', col('open').cast(FloatType()))
            .withColumn('volume', col('volume').cast(FloatType()))
            .withColumn('dividends', col('dividends').cast(IntegerType()))
            .withColumn('stock_splits', col('stock_splits').cast(IntegerType()))
            )
        # Saving
        extract_at = start.replace('-', '_')
        dataset.write.format('parquet') \
            .mode('overwrite') \
        .save(os.path.join(DIR_PATH_RAW_STOCK, f'extracted_{extract_at}_stock.parquet'))
        dataset.filter(col('ticker')=='BBAS3.SA').show()                                                                         
    except:
            print('No data found, symbol may be delisted')




if __name__ == "__main__":
  
    parser = argparse.ArgumentParser(
        description="Spark Pre-processing"
    )
    parser.add_argument("--ticker_list_type", required=True)
    parser.add_argument("--start", required=True)
    args = parser.parse_args()
    
    print(f'Data de start: {args.start}')
    get_stock_information(ticker_list=args.ticker_list_type ,start=args.start)
