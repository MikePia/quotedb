"""
Using the finnhub candles endpoint cycle through stocks. When the requirements become
clearer, add commandline args with date shortcuts and possibility of reading a file for
tickers

"""
import pandas as pd
from quotedb.finnhub.finncandles import FinnCandles
from quotedb.getdata import startCandles
from quotedb.models.candlesmodel import CandlesModel
# from quotedb.sp500 import getQ100_Sp500, nasdaq100symbols
from quotedb.utils.util import dt2unix

if __name__ == '__main__':
    # stocks = getQ100_Sp500()
    fc = FinnCandles([])
    stocks = sorted(fc.getSymbols())
    # stocks = sorted(nasdaq100symbols)
    # stocks = ['CHTR', 'CVX', 'CMG', 'CB', 'CHD', 'CI', 'CINF', 'CTAS', 'CSCO',
    # '         C', 'CFG', 'CTXS', 'CLX', 'CME', 'CMS', 'KO', 'CTSH', 'CL', 'CMCSA', 'CMA']

    # start = dt2unix(pd.Timestamp(2021,  3, 15, 15, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    start = dt2unix(pd.Timestamp(2021, 3, 1, 9, 30).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))

    startCandles(stocks, start, CandlesModel, latest=True)
