"""
Using the finnhub candles endpoint cycle through stocks. When the requirements become
clearer, add commandline args with date shortcuts and possibility of reading a file for
tickers

"""
import pandas as pd
from stockdata.getdata import startCandles
from stockdata.sp500 import getQ100_Sp500
from utils.util import dt2unix

if __name__ == '__main__':
    stocks = getQ100_Sp500()
    start = dt2unix(pd.Timestamp(2021,  3, 15, 15, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))

    startCandles(stocks, start)
