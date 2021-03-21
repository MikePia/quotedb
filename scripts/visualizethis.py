"""
A wrapper for getCurrentDataFile to run from the command line. Don't know if this is going
to be used. If so, command line arguments will be implemented
"""
import pandas as pd
from stockdata.finnhub.finncandles import FinnCandles
from utils.util import dt2unix
from stockdata.getdata import getCurrentDataFile


if __name__ == '__main__':
    fc = FinnCandles([])
    stocks = fc.getSymbols()
    # stocks.append('BINANCE:BTCUSDT')
    # startdelt = dt.timedelta(days=75)

    startdelt = pd.Timestamp(2021, 3, 17, 13, 45).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None)
    # startdelt = dt.datetime(2021, 1, 1)
    fn = 'thedatafile.json'
    gltime = dt2unix(pd.Timestamp(2021,  3, 15, 12, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    numrec = 10
    getCurrentDataFile(stocks, startdelt, fn, (gltime, numrec), format='visualize', bringtodate=False)
