import csv
import datetime as dt
import json
import os.path

from stockdata.dbconnection import getSaConn, getCsvDirectory

from models.candlesmodel import ManageCandles, CandlesModel
from models.finntickmodel import ManageFinnTick, FinnTickModel
from models.trademodel import ManageTrade, TradeModel
from models.polytrademodel import ManagePolyTrade, PolyTradeModel

from stockdata.finnhub.stockquote import StockQuote
from stockdata.finnhub.trades import MyWebSocket
from stockdata.polygon.polytrade import PolygonApi
from stockdata.sp500 import nasdaq100symbols
from utils.util import dt2unix


def getCurrentDataFile(stocks, startdelt, fn, format='json', bringtodate=False):
    """
    Explanation
    -----------
    Create a data file of trades that continues to update itself for current trades

    Parameters
    ----------
    :params stocks: list<str>: List of stocks
    :params startdelt : [dt.timedelt, dt.datetime, int]:
        A time value to indicate the beginning pont. A delta indicates time before now, A datetime or unix int
        indicate precise UTC time
    :params fn: str: File name. Directory ocation is is defined internally (getCsvLocataion()). Timestamp will be added to name
    :params format: str: json or csv
    :params bringtodate : bool: If True, override startdelt and begin each stock from it's latest entry if ther is one
    """
    if bringtodate:
        # Note I think in production, a seperate running program will be updataing
        # this continuously
        startCandles(stocks, None, latest=True, numcycles=0)

    # Allow startdelt to be timedelta, datetime or unix time (int)
    start = None
    if isinstance(startdelt, dt.timedelta):
        start = dt2unix(dt.datetime.utcnow() - startdelt, unit='s')
    elif isinstance(startdelt, dt.datetime):
        start = dt2unix(startdelt, unit='s')
    elif isinstance(startdelt, int):
        start = startdelt

    end = dt2unix(dt.datetime.utcnow(), unit='s')
    j = getCandles(stocks, start, end, format=format)
    fn = f'{getCsvDirectory()}/{fn}'
    fn = os.path.splitext(fn)[0]
    fmat = '.csv' if format.lower() == 'csv' else '.json'
    d = dt.datetime.now()
    fn = f'{fn}_{d.strftime("%Y%m%d_%H%M%S")}{fmat}'
    if format == 'json':
        with open(fn, 'w') as f:
            f.write(j)
    elif format == 'csv':
        with open(fn, 'w') as f:
            writer = csv.writer(f)
            for row in j:
                writer.writerow(row)

    stocks = filterStocks(stocks)
    startTickWS(stocks, store=[format], fn=fn)

    print(len(j))
    print()


def filterStocks(stocks, filter={'pricedif': 0}):
    '''
    Place holder
    '''
    return stocks


def getCandles(stocks, start, end, format='json'):
    '''
    Explanaition
    ------------
    Get candles from the database candles table

    Parameters
    ----------
    :params stocks: List of tickers
    :params start: int (unix date in seconds) or datetime type
    :params end: int (unix date in seconds) or datetime type
    '''
    # from stockdata.dbconnection import getCsvDirectory
    start = start if isinstance(start, int) else dt2unix(start)
    end = end if isinstance(end, int) else dt2unix(end)
    mk = ManageCandles(getSaConn(), True)
    x = CandlesModel.getTimeRangeMultipleVpts(stocks, start, end, mk.session)
    if not x:
        return {}
    if format == 'json':
        return json.dumps(x)
    # Return list of lists for csv
    return [[t['symbol'], t['price'], t['time'], t['volume']] for t in x]


def startCandles(stocks, start, latest=False, numcycles=9999999999):
    sq = StockQuote(stocks, None)
    sq.cycleStockCandles(start, latest, numcycles)


def getTicks(stocks, start, end, api='fh', format='json'):
    mt = ManageTrade(getSaConn(), create=True)

    start = dt2unix(start, unit='m')
    end = dt2unix(end, unit='m')
    x = TradeModel.getTimeRangeMultiple(stocks, start, end, mt.session)
    if not x:
        return {}
    xlist = [z.__dict__ for z in x]
    for xd in xlist:
        del xd['_sa_instance_state']

    j = json.dumps(xlist)
    print(len(x))
    return j


def startTickWS(stocks, store=['db'], fn=None):
    MyWebSocket(stocks, store=store, fn=fn)


def getTicksREST(stocks, start, end):
    mft = ManageFinnTick(getSaConn())
    start = dt2unix(start, unit='m')
    end = dt2unix(end, unit='m')
    x = FinnTickModel.getTimeRangeMultiple(stocks, start, end, mft.session)
    if not x:
        return {}
    xlist = [z.__dict__ for z in x]
    for xd in xlist:
        del xd['_sa_instance_state']

    j = json.dumps(xlist)
    print(len(x))
    return j


def startLastXTicks(stocks, dadate, delt):
    """
    :params stocks: list<str>
    :params dadate: date
    :params delt: pandas.Timedelta
    """
    sq = StockQuote(stocks, dadate, limit=25000)
    sq.cycleStockTicks(startat=delt)


def getPolyTrade(stocks, start, end):
    mt = ManagePolyTrade(getSaConn())

    x = PolyTradeModel.getTimeRangeMultiple(stocks, start, end, mt.session)
    if not x:
        return {}
    xlist = [z.__dict__ for z in x]
    for xd in xlist:
        del xd['_sa_instance_state']

    j = json.dumps(xlist)
    print(len(x))
    return j


def startPolyTrade(stocks, tdate, start, filternull=True):
    pa = PolygonApi(stocks, tdate, start, filternull=filternull)
    pa.cycleStocksToCurrent()


# endpoint does not give current timestamp with the quote. The timestamp
# it does give is always 7PM on the the day of the quote. Weird because the
# Quote price updates but the time does not.
def startGetQuotes(stocks, start, stop, freq):
    '''
    Useless
    '''
    sq = StockQuote(stocks, None)

    sq.cycleQuotes(start, stop, freq, store=True)


if __name__ == "__main__":
    # stocks = ["PDD", "ROKU", "ROST", "SBUX", "SIRI", "SWKS", "TCOM", "TXN", "VRSK", "VRSN", "VRTX", "WBA", "WDAY", "XEL", "XLNX", "ZM", ]
    # stocks = ['AAPL', 'AMZN', 'ROKU', 'GME', 'TSLA', 'BB', 'SQ', 'MU', 'BINANCE:BTCUSDT']
    # tz = 'US/Eastern'
    # start = pd.Timestamp("2021-3-11 11:30", tz=tz).tz_convert("UTC").replace(tzinfo=None)
    # end = pd.Timestamp("2021-3-11 12:00", tz=tz).tz_convert("UTC").replace(tzinfo=None)
    ########################################
    # stocks = nasdaq100symbols
    # start = dt.datetime.utcnow()
    # stocks = random50(numstocks=50)
    # startCandles(stocks, None, latest=True)
    # x = getCandles(stocks, start, end)
    #########################################
    # # x = getTicks(stocks, start, end)
    # # MyWebSocket(stocks)
    # dadate = dt.date(2021, 3, 11)
    # delt = pd.Timedelta(minutes=30)
    # # startLastXTicks(stocks, dadate, delt)
    # x = getTicksREST(stocks, start, end)
    ########################

    # stocks = getQ100_Sp500()
    # start = dt.datetime.utcnow()
    # stop = dt.datetime.utcnow() + dt.timedelta(seconds=180)
    # freq = 20
    # startGetQuotes(stocks, start, stop, freq)

    #######################################
    # stocks = nasdaq100symbols
    # start = dt2unix(dt.datetime.utcnow() - dt.timedelta(hours=3), 'n')
    # tdate = dt.date(2021, 3, 12)
    # startPolyTrade(stocks, tdate, start)

    # print('done')
    #########################################
    # stocks = None
    # start = dt2unix(dt.datetime.utcnow() - dt.timedelta(hours=3), unit='n')
    # end = dt2unix(dt.datetime.utcnow(), unit='n')
    # j = getPolyTrade(stocks, start, end)
    ########################################
    stocks = nasdaq100symbols
    stocks.append('BINANCE:BTCUSDT')
    startdelt = dt.timedelta(hours=24)
    fn = 'thedatafile.json'
    getCurrentDataFile(stocks, startdelt, fn, format='csv', bringtodate=False)

    ##############################################
    # stocks = nasdaq100symbols
    # # enable woking in off hours with some data from finnhub
    # stocks.append('BINANCE:BTCUSDT')
    # startTickWS(stocks, store=['json'], fn=f'{getCsvDirectory()}/ws_json.json')
