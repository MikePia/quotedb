"""
This is a module of APIs. All the objects are functions that instantiate the classes
that do the work. These functions will be the published API for the client to use.
"""

import datetime as dt
import json
import logging
import time

import pandas as pd

from quotedb.dbconnection import getSaConn
from quotedb.finnhub.finncandles import FinnCandles
from quotedb.finnhub.finntrade_ws import MyWebSocket, ProcessData
from quotedb.finnhub.stockquote import StockQuote
from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.common import createFirstQuote

from quotedb.models.candlesmodel import CandlesModel
from quotedb.models.finntickmodel import FinnTickModel, ManageFinnTick
from quotedb.models.firstquotemodel import Firstquote
from quotedb.models.managecandles import ManageCandles
from quotedb.models.polytrademodel import ManagePolyTrade, PolyTradeModel
from quotedb.models.trademodel import ManageTrade, TradeModel
from quotedb.polygon.polytrade import PolygonApi
from quotedb.utils import util


def getCurrentDataFile(stocks, startdelt, fn, start_gl, model=CandlesModel, format='json', bringtodate=False):
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
        since the time 1609459200 (2021/01/01 utc). The result will only be accurate if the db already contains
        the data.
    :params format: str: json or csv
    :params bringtodate : bool: If True, override startdelt and begin each stock from it's latest entry if ther is one
    """
    # Allow startdelt to be timedelta, datetime or unix time (int)
    start = None
    if isinstance(startdelt, dt.timedelta):
        start = util.dt2unix(dt.datetime.utcnow() - startdelt, unit='s')
    elif isinstance(startdelt, dt.datetime):
        start = util.dt2unix(startdelt, unit='s')
    elif isinstance(startdelt, int):
        start = startdelt

    if bringtodate:
        # Note I think in production, a seperate running program will be updataing
        # this continuously
        startCandles(stocks, start, model, latest=True, numcycles=0)

    end = util.dt2unix(dt.datetime.utcnow(), unit='s')
    df = getCandles(stocks, start, end, model=model)
    ffn = util.formatFn(fn, format)
    util.writeFile(util.formatData(df, format), ffn, format)

    gainers, losers = localFilterStocks(df, stocks, start_gl)
    # gainers, losers = filterStocks(stocks, {'pricediff': start_gl}, model)  # TODO figure how to speed this call up. Thread? Stored procedure?
    gainers.extend(losers[1:])
    gainers = [x[0] for x in gainers][1:]
    # gainers.append('BINANCE:BTCUSDT')

    ws_thread = startTickWS(gainers, store=[format], fn=ffn)

    while True:
        cur = time.time()
        nexttime = cur + 240

        while time.time() < nexttime:
            if not ws_thread.is_alive():
                print('Websocket was stopped: restarting...')
                ws_thread = startTickWS([x[0] for x in gainers][1:], store=[format], fn=ffn)
            print(' ** ')
            time.sleep(5)
        start_gl = (start_gl[0] + (10 * 60), start_gl[1]+1)
        df = getCandles(stocks, start, end)
        ffn = util.formatFn(fn, format)
        util.writeFile(util.formatData(df, format), ffn, format)

        gainers, losers = localFilterStocks(df, stocks, start_gl)
        # gainers, losers = filterStocks(stocks, {'pricediff': start_gl})  # TODO figure how to speed this call up. Thread? Stored procedure?
        gainers.extend(losers[1:])
        gainers = [x[0] for x in gainers][1:]
        # gainers.append('BINANCE:BTCUSDT')
        if gainers:
            ws_thread.changesubscription(gainers, newfn=ffn)


def localFilterStocks(df, stocks, gl):
    '''
    Explanation
    -----------
    A version of getGainersAndLoser that wirks on an existing object.

    Paramaters
    ----------
    :params df: DataFrame: Must include a field labeled 'price' or 'close' and a field labeled 'timestamp'
    :params stock: list: str
    :params gl:tuple(start, numstocks)
    '''
    if df.empty:
        return [], []
    gainers = []
    losers = []
    cols = df.columns
    price_key = 'close' if 'close' in cols else 'price' if 'price' in cols else None
    if not price_key or 'timestamp' not in cols:
        logging.error("Invalid data format")
        raise ValueError("Invalid data formt")
    for tick in df.stock.unique():
        t = df[df.stock == tick]
        t = t.copy()
        t.sort_values(['timestamp'], inplace=True)

        firstprice, lastprice = t.iloc[0][price_key], t.iloc[-1][price_key]
        pricediff = firstprice - lastprice
        percentage = abs(pricediff / firstprice)
        if pricediff >= 0:
            gainers.append([tick, pricediff, percentage, firstprice, lastprice])
        else:
            losers.append([tick, pricediff, percentage, firstprice, lastprice])

    gainers.sort(key=lambda x: x[2], reverse=True)
    losers.sort(key=lambda x: x[2], reverse=True)
    gainers = gainers[:gl[1]]
    losers = losers[:gl[1]]
    gainers.insert(0, ['stock', 'pricediff', 'percentage', 'firstprice', 'lastprice'])
    losers.insert(0, ['stock', 'pricediff', 'percentage', 'firstprice', 'lastprice'])
    return gainers, losers


def getJustGainersLosers(start, end,  stocks, numrec, model=AllquotesModel, local=True, fulldata=False):
    """
    Explanation
    -----------
    Simplified to return just a list of gainers/losers. If the percantages are needed,
    use the the underlying calls.

    Paramaters
    ----------
    :params start: int: unix time
    :params end: int: unix time
    :params stocks: list
    :params numrec: int
    :params model: [AllquotesModel, CandlesModel]
    :params local: bool: The local version gets the candles and analyzes in python
        The non-local version uses a SQL select. The faster version will depend on
        how the database is tuned and other factors.
    :params return: list<str>: A list of tickers or list of [stock, diff, pct, last, first]
    """

    if local:
        df = getCandles(stocks, start, end, model=model)
        gainers, losers = localFilterStocks(df, stocks, (start, numrec))
    else:
        print('begin getGainersLosers')
        gainers, losers = getGainersLosers(stocks, start, numrec, model=model)

    gainers.extend(losers[1:])
    if fulldata:
        return gainers[1:]

    return [x[0] for x in gainers][1:]


def filterStocks(stocks, filter, model=CandlesModel):
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
        gainers, losers = getGainersLosers(stocks, filter['pricediff'][0], filter['pricediff'][1], model)
        return gainers, losers
    logging.info('No filters were applied')
    return stocks


def getCandles(stocks, start, end, model=AllquotesModel):
    '''
    Explanation
    ------------
    Get candles from the database candles table

    Parameters
    ----------
    :params stocks: List of tickers
    :params start: int (unix date in seconds) or datetime type
    :params end: int (unix date in seconds) or datetime type
    '''

    start = start if isinstance(start, int) else util.dt2unix(start)
    end = end if isinstance(end, int) else util.dt2unix(end)
    mk = ManageCandles(getSaConn, model, True)
    df = mk.getTimeRangeMultipleVpts(stocks, start, end)
    if df.empty:
        return pd.DataFrame()
    return df

    # if format == 'json':
    #     return df.to_json()
    # # Return list of lists for csv
    # return df.to_numpy().tolist()


def startCandles(stocks, start, model=CandlesModel, latest=False, numcycles=9999999999):
    """
    Explanation
    -----------
    Cycle through stocks, retrieve data from finnhub  and save it to the database

    Paramaters
    ----------
    :params stocks: list<str>: List of stocks
    :params start: int: Unix time. Get data beginning at this time
    :params model: A sqlalchemy model: Currently one of [AllquotesModel, TopquotesModel, CandleModel]
    :params latest: bool: If True, Get the latest data in the table for each stock, if it is greater than
        the start value, begin collection ther instead of start
    :params numcycles: int: Will stop execution upon completion of cycles.
    """
    if isinstance(start, dt.datetime):
        start = util.dt2unix(start)
    fc = FinnCandles(stocks)
    fc.cycleStockCandles(start, model, latest, numcycles)
    return fc


def getTicks(stocks, start, end, api='fh', format='json'):
    mt = ManageTrade(getSaConn(), create=True)

    start = util.dt2unix(start, unit='m')
    end = util.dt2unix(end, unit='m')
    x = TradeModel.getTimeRangeMultiple(stocks, start, end, mt.session)
    if not x:
        return {}
    xlist = [z.__dict__ for z in x]
    for xd in xlist:
        del xd['_sa_instance_state']

    j = json.dumps(xlist)
    print(len(x))
    return j


def startTickWS(stocks, fn='datafile', store=['csv'], delt=None):
    mws = MyWebSocket(stocks, fn, store=store, resample_td=delt,)
    mws.start()
    return mws


def startTickWSKeepAlive(stocks, fn, store, delt=None, polltime=5):

    ws_thread = startTickWS(stocks, fn=fn, store=store, delt=delt)

    while ws_thread.keepgoing:
        cur = time.time()
        nexttime = cur + 5

        while time.time() < nexttime and ws_thread.keepgoing:
            if not ws_thread.is_alive() and ws_thread.keepgoing:
                print('Websocket was stopped: restarting...')

                ws_thread = startTickWS(stocks, store=[format], fn=fn)
            print(' ** ')
            time.sleep(polltime)

        # This is where a new gainers could be found new subscription could be called


def startTickWS_SampleFill(stocks, fn, fq, delt=dt.timedelta(seconds=0.25), polltime=5):
    """
    Explanation
    -----------
    Calls the finnhub websocket and subscribes to stocks. Writes the file fn{timestamp}.json
    and continues to add to it until stop. Uses the Firstquote (fq) to provide delta information.
    The format of the output is resampled to {delt} and each tick has an entry for enach stock

    Paramaters
    ----------
    :params stocks: list: of stocks
    :params fn: str: filename
    :params fq: [int<unixtime>, Firstquote]: If fq is an int, a Firstquote object will be retrieved or created for it.
    :params delt: timedelta: This is used for the resampling value. 1/4 second by default
    :params polltime: int: Seconds between a keepalive call for the websocket.  If the connection fails, it will be restarted
    """
    fn = util.formatFn(fn, format='json')
    resample_td = delt
    store = ['visualize']

    if isinstance(fq, int):
        d = fq
        fq = createFirstQuote(d, AllquotesModel, stocks=stocks, usecache=True)
        if set([x.stock for x in fq.firstquote_trades]) != set(stocks):
            fq = createFirstQuote(d, AllquotesModel, stocks=stocks, usecache=False)
        print()
    mws = MyWebSocket(stocks, fn, store=store, resample_td=resample_td, fq=fq, ffill=True)
    mws.start()
    while mws.keepgoing:
        if not mws.is_alive() and mws.keepgoing:
            print('Websocket was stopped: restarting...')
            mws = MyWebSocket(stocks, fn, store=store, resample_td=resample_td, fq=fq, ffill=True)
            mws.start()
        time.sleep(polltime)
        print(' ** ')


def getTicksREST(stocks, start, end):
    mft = ManageFinnTick(getSaConn())
    start = util.dt2unix(start, unit='m')
    end = util.dt2unix(end, unit='m')
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


def getGainersLosers(tickers, start, numstocks, model=CandlesModel):
    """
    Explanation
    ------------
    Filter the stocks in {tickers} for the largest price difference since the time {start}
    Parameters
    __________
    :params tickers: List<str>. Filter result by tickers, If empty [], don't filter
    :params start: int: Unix time in seconds
    :params numstocks: The number of stocks to include in gainers and losers
    :return: (list<list>, list<list>): (gainers, losers): Each sub list is [stock, pricediff, percentagediff, firstprice, lastprice]
    """
    mc = ManageCandles(getSaConn(), model)
    return mc.filterGainersLosers(tickers, start, numstocks)


def createFirstquote(timestamp, model=AllquotesModel, stocks='all'):
    """
    Explanation
    ------------
    Create an entry in the firstquote table fro {timestamp} and related firstquote_trades. The result will
    depend on the data that is already gathered in the table represented by {model}. If the {timestamp} is already
    in the table add any new entries found in the database since it was last modified.

    Parameters
    ----------
    :params timestamp: int: Unix timestamp in seconds. utc time
    :params model: A sqlalchemy model: Currently one of [AllquotesModel, TopquotesModel, CandleModel]
    """
    return createFirstQuote(timestamp, model, stocks)


def getFirstQuote(timestamp, wiggle=0, model=AllquotesModel):
    """
    Explanation
    -----------
    Either retrives an existing Firstquote or creates and retrieves a new one

    Paramaters
    -----------
    :params timestamp: int Unix time
    :params wiggle: int: In seconds, The number of seconds leeway to accept a Firstquote if it exists
    :params model: One of [CandlesModel, AllquotesModel, TopquotesModel]
    :params return: A Firstquote object

    Programming Notes
    -----------------
    Have published the two methods in quotedb.models.common as the the API to use.
    This works fine. test upcoming :) and is used internally. (Need to evaluate that)
    But the methods in common are better for an API.
    """
    from quotedb.models.metamod import getSession
    s = getSession()
    fqs = Firstquote.availFirstQuotes(timestamp-wiggle, timestamp, s)

    if fqs:
        return fqs[-1]
    return createFirstquote(timestamp, model=model)


def getDeltaData(stocks, start, end, fq, model=AllquotesModel, format="df"):
    if fq is None:
        fq = getFirstQuote(start, model=model)
    mc = ManageCandles(getSaConn(), model)
    x = mc.getDeltaData(stocks, start, end, fq, format)
    return x


def visualizeData(fn, fq, delt=dt.timedelta(seconds=10)):
    if isinstance(delt, int):
        delt = dt.timedtlta(seconds=delt)
    proc = ProcessData(None, None, dt.timedelta(seconds=10))
    fn = proc.latestfile(fn)
    if not fn:
        raise ValueError("Not file found")
    return proc.visualizeData(fn, fq)


if __name__ == "__main__":
    #########################################
    from quotedb import sp500
    stocks = sp500.getSymbols()
    start = util.dt2unix_ny(dt.datetime(2021, 5, 17, 9, 30))
    end = util.dt2unix(dt.datetime.utcnow())
    model = CandlesModel
    numrec = 50

    # df = getCandles(stocks, start, end)
    # # gl = localFilterStocks(df, stocks, (start, numrec))
    # # print(len(gl[0]), len(gl[1]))
    # gainers, losers = getGainersLosers(stocks, start, 50, AllquotesModel)\
    stocks = getJustGainersLosers(start, end, [], numrec, model, local=False)
    print(stocks)
    fn = 'thatlldopig'
    startTickWSKeepAlive(stocks, fn, store=['json'], delt=None, polltime=5)
    # print(len(gainers), len(losers))
    #########################################

    # fq = util.dt2unix_ny(dt.datetime(2021, 4, 29, 8, 30))
    # fn = r'^x_\d\d?_report_json'
    # fn = "x_15_report_json_20210422_145929.json"
    # fn = "mockbiz_20210429_165437.json"

    # jdat = visualizeData(fn, fq)
    #########################################
    # from quotedb.utils.util import dt2unix_ny
    # stocks = random50(numstocks=20)
    # start = dt2unix_ny(dt.datetime(2021, 4, 9, 9, 30))
    # fn = 'visualizetodqay.json'
    # numrec = 10

    # getCurrentDataFile(stocks, start, fn, (start, numrec), model=AllquotesModel, format='visualize', bringtodate=False)
