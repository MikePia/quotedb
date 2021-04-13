import datetime as dt
from quotedb.getdata import startCandles
from quotedb.sp500 import nasdaq100symbols
from quotedb.utils.util import dt2unix_ny


start = dt2unix_ny(dt.datetime(2021, 4, 1))


startCandles(nasdaq100symbols, start)