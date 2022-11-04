import argparse
from operator import index


def get_stock_information(ticker_list, start, end=None):
    
    import os
    from datetime import datetime, timedelta
    import yfinance as yf
    from pyspark.context import SparkContext
    from pyspark.conf import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import to_date, col, lit
    from sparkDocuments import schema_ticker, DIR_PATH_RAW_STOCK
    
    def _get_end(start):

        start = datetime.strptime(start, '%Y-%m-%d').date()
        end = start + timedelta(1)
        end = end.strftime('%Y-%m-%d')
        
        return end

    sk = SparkSession(SparkContext(conf=SparkConf())\
    .getOrCreate())
    
    if end is None:
        end = _get_end(start=start)

    try:
        varlist = ['date', 'ticker', 'adj_close', 'close', 'high', 'low', 'open', 'volume']
        dataset = yf.download(ticker_list, start=start, end=end)
        dataset = dataset.stack().reset_index()
        dataset.columns = varlist
        dataset['date']=dataset['date'].astype(str)
        if dataset.empty:
            raise Exception
        dataset = sk.createDataFrame(dataset)
        dataset = dataset.filter(col('date') == start)
    except:
            print('No data found, symbol may be delisted')

    # Saving
    extract_at = start.replace('-', '_')
    dataset.write.format('parquet') \
        .mode('overwrite') \
       .save(os.path.join(DIR_PATH_RAW_STOCK, f'extracted_{extract_at}_stock.parquet'))  
 
if __name__ == "__main__":
  
    parser = argparse.ArgumentParser(
        description="Spark Pre-processing"
    )
    parser.add_argument("--start", required=True)
    args = parser.parse_args()

    from sparkDocuments import ticker_list
    get_stock_information(ticker_list=ticker_list, start=args.start)