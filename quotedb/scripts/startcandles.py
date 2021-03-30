"""
Using the finnhub candles endpoint cycle through stocks. When the requirements become
clearer, add commandline args with date shortcuts and possibility of reading a file for
tickers

"""
import pandas as pd
from quotedb.getdata import startCandles

# from quotedb.models.candlesmodel import CandlesModel
from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.topquotes_candlemodel import TopquotesModel
from quotedb.sp500 import nasdaq100symbols, getSymbols
from quotedb.utils.util import dt2unix

if __name__ == '__main__':
    stocks = sorted(nasdaq100symbols)
    stocks = getSymbols()

    start = dt2unix(pd.Timestamp(2021, 3, 25, 9, 30).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    # model = TopquotesModel
    model = AllquotesModel
    startCandles(stocks, start, model, latest=True)
