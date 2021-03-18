"""
This is a module of APIs. All the objects are functions that instantiate the classes
that do the work. These functions will be the published API for the client to use.
"""

import datetime as dt
import json
import logging
import time

from models.candlesmodel import CandlesModel, ManageCandles
from models.finntickmodel import FinnTickModel, ManageFinnTick
from models.polytrademodel import ManagePolyTrade, PolyTradeModel
from models.trademodel import ManageTrade, TradeModel
from utils.util import dt2unix, formatFn, writeFile

from stockdata.dbconnection import getSaConn
from stockdata.finnhub.finncandles import FinnCandles
from stockdata.finnhub.stockquote import StockQuote
from stockdata.finnhub.finntrade_ws import MyWebSocket
from stockdata.polygon.polytrade import PolygonApi
from stockdata.sp500 import getQ100_Sp500


def getCurrentDataFile(stocks, startdelt, fn, start_gl, format='json', bringtodate=False):
    """
    Explanation
    -----------
    Create a data file of trades that continues to update itself for current trades

    Parameters
    ----------
    :params stocks: list<str>: List of stocks
    :params startdelt : [dt.timedelt, dt.datetime, int]:
        A time value to indicate the beginning point. A delta indicates time before now, A datetime or unix int
        indicates a starttime (UTC)
    :params fn: str: File name. Directory ocation is is defined internally (getCsvLocataion()). Timestamp will be added to name
    :params start_gl: tuple(int, int): (Unix time, numberStocks)
        Define the gainers/losers filter for example (1609459200, 10) means to get the top 10 gainers and losers
        since the time 1609459200 (2021/01/01 utc). The result will only be accurate only if the db already contains
        the data.
    :params format: str: json or csv
    :params bringtodate : bool: If True, override startdelt and begin each stock from it's latest entry if ther is one
    """
    # Allow startdelt to be timedelta, datetime or unix time (int)
    start = None
    if isinstance(startdelt, dt.timedelta):
        start = dt2unix(dt.datetime.utcnow() - startdelt, unit='s')
    elif isinstance(startdelt, dt.datetime):
        start = dt2unix(startdelt, unit='s')
    elif isinstance(startdelt, int):
        start = startdelt

    if bringtodate:
        # Note I think in production, a seperate running program will be updataing
        # this continuously
        startCandles(stocks, start, latest=True, numcycles=0)

    end = dt2unix(dt.datetime.utcnow(), unit='s')
    j = getCandles(stocks, start, end, format=format)
    ffn = formatFn(fn, format)
    writeFile(j, ffn, format)

    fstocks = filterStocks(stocks, {'pricediff': start_gl})  # TODO figure how to speed this call up. Thread? Stored procedure?
    fstocks[0].extend(fstocks[1])
    ws_thread = startTickWS([x[0] for x in fstocks[0]][1:], store=[format], fn=ffn)
    while True:
        cur = time.time()
        nexttime = cur + 240

        while time.time() < nexttime:
            if not ws_thread.is_alive():
                print('Websocket was stopped: restarting...')
                ws_thread = startTickWS([x[0] for x in fstocks[0]][1:], store=[format], fn=ffn)
            print(' ** ')
            time.sleep(5)
        start_gl = (start_gl[0] + (10 * 60), start_gl[1]+1)
        getCandles(stocks, start, end, format=format)
        ffn = formatFn(fn, 'csv')
        writeFile(j, ffn, format)

        fstocks = filterStocks(stocks, {'pricediff': start_gl})  # TODO figure how to speed this call up. Thread? Stored procedure?
        fstocks[0].extend(fstocks[1])
        ws_thread.changesubscription([x[0] for x in fstocks[0][1:]], newfn=ffn)


def filterStocks(stocks, filter):
    '''
    Explanation
    -----------
    provide filters for the stocks

    Paramaters
    ----------
    :params stocks: A super set of stocks.
    :params filter: dict
        pricediff filter
    '''
    if filter.get('pricediff'):
        gainers, losers = getGainersLosers(stocks, filter['pricediff'][0], filter['pricediff'][1])
        return gainers, losers
    logging.info('No filters were applied')
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
    df = CandlesModel.getTimeRangeMultipleVpts(stocks, start, end, mk.session)
    if df.empty:
        return {}
    if format == 'json':
        return df.to_json()
    # Return list of lists for csv
    return df.to_numpy().tolist()


def startCandles(stocks, start, latest=False, numcycles=9999999999):
    fc = FinnCandles(stocks)
    fc.cycleStockCandles(start, latest, numcycles)


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


def startTickWS(stocks, fn='datafile', store=['csv']):
    mws = MyWebSocket(stocks, fn, store=store)
    mws.start()
    return mws


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


def getGainersLosers(tickers, start, numstocks):
    """
    Explanaition
    ------------
    Filter the stocks in {tickers} for the largest price difference since the time {start}
    Parameters
    __________
    :params tickers: List<str>
    :params start: int: Unix time in seconds
    :params numstocks: The number of stocks to include in gainers and losers
    :return: (list<list>, list<list>): (gainers, losers): Each sub list is [symbol, pricediff, percentagediff, firstprice, lastprice]
    """
    mc = ManageCandles(getSaConn())
    return mc.filterGanersLosers(tickers, start, numstocks)


if __name__ == "__main__":
    # stocks = ["PDD", "ROKU", "ROST", "SBUX", "SIRI", "SWKS", "TCOM", "TXN", "VRSK", "VRSN", "VRTX", "WBA", "WDAY", "XEL", "XLNX", "ZM", ]
    # stocks = ['AAPL', 'AMZN', 'ROKU', 'GME', 'TSLA', 'BB', 'SQ', 'MU', 'BINANCE:BTCUSDT']
    # tz = 'US/Eastern'
    # start = pd.Timestamp("2021-3-11 11:30", tz=tz).tz_convert("UTC").replace(tzinfo=None)
    # end = pd.Timestamp("2021-3-11 12:00", tz=tz).tz_convert("UTC").replace(tzinfo=None)
    ########################################
    # stocks = nasdaq100symbols
    # # start = dt.datetime.utcnow()
    # # start = dt.datetime.utcnow() - dt.timedelta(days=60)
    # start = dt.datetime(2021, 1, 1)

    # stocks = nasdaq100symbols
    # # stocks = ['AAPL', 'SQ']
    # startCandles(stocks, start, latest=True)
    # x = getCandles(stocks, start, None)
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
    import pandas as pd
    fc = FinnCandles([])
    stocks = fc.getSymbols()
    # stocks.append('BINANCE:BTCUSDT')
    # startdelt = dt.timedelta(days=75)

    startdelt = pd.Timestamp(2021, 3, 17, 13, 45).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None)
    # startdelt = dt.datetime(2021, 1, 1)
    fn = 'thedatafile.json'
    gltime = dt2unix(pd.Timestamp(2021,  3, 15, 12, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    numrec = 10
    getCurrentDataFile(stocks, startdelt, fn, (gltime, numrec), format='csv', bringtodate=False)

    ##############################################
    # stocks = nasdaq100symbols
    # # enable woking in off hours with some data from finnhub
    # stocks.append('BINANCE:BTCUSDT')
    # startTickWS(stocks, store=['json'], fn=f'{getCsvDirectory()}/ws_json.json')
    ##############################################
    # import pandas as pd
    # from pprint import pprint
    # start = dt2unix(pd.Timestamp(2021,  3, 12, 12, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    # stocks = nasdaq100symbols
    # gainers, losers = getGainersLosers(stocks, start)
    # pprint(gainers)
    # print()
    # pprint(losers)
