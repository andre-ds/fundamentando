import os
import investpy
import yfinance as yf
import pandas as pd
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from sparkFiles.PreProcessing import PreProcessing
from pyspark.sql.types import IntegerType, FloatType

sk = SparkSession(SparkContext(conf=SparkConf())\
.getOrCreate())

pp = PreProcessing(spark_environment=sk)

DIR_PATH = os.path.join(os.path.dirname(os.path.realpath('__file__')), 'datalake')
DIR_PATH_PROCESSED_FCA_STOCK_TYPE = os.path.join(DIR_PATH, 'pre-processed-fca-stock-type')
DIR_PATH_PROCESSED_STOCK = os.path.join(DIR_PATH, 'pre-processed-stock')


ticker_list = 'file'
stock_type_file = 'register_2023_01_27_stock_tickers.csv'
start = '2000-01-01'
end = '2023-01-29'


'''
The goal of this code is make the download of a larger period of time of stock's price's.
Therefore, none DAG uses this code.
'''

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
    dataset_stocks = pd.read_csv(os.path.join(DIR_PATH_PROCESSED_FCA_STOCK_TYPE, stock_type_file), dtype=dtype)
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

    # Saving
    reference_date = start.replace('-', '_')
    if dataset.isEmpty() == False:
        dataset.write.format('parquet') \
            .mode('overwrite') \
        .save(os.path.join(DIR_PATH, 'pp_stock_union.parquet'))                                                          
except:
        print('No data found, symbol may be delisted')
