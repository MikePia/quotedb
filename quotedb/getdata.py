"""
This is a module of APIs. All the objects are functions that instantiate the classes
that do the work. These functions will be the published API for the client to use.
"""

import datetime as dt
import json
import logging
import time

import pandas as pd

from quotedb.dbconnection import getCsvDirectory, getSaConn
from quotedb.finnhub.finncandles import FinnCandles
from quotedb.finnhub.finntrade_ws import MyWebSocket
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
from quotedb.sp500 import random50
from quotedb.utils.util import dt2unix, formatData, formatFn, writeFile

# from quotedb.sp500 import getQ100_Sp500


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
        start = dt2unix(dt.datetime.utcnow() - startdelt, unit='s')
    elif isinstance(startdelt, dt.datetime):
        start = dt2unix(startdelt, unit='s')
    elif isinstance(startdelt, int):
        start = startdelt

    if bringtodate:
        # Note I think in production, a seperate running program will be updataing
        # this continuously
        startCandles(stocks, start, model, latest=True, numcycles=0)

    end = dt2unix(dt.datetime.utcnow(), unit='s')
    df = getCandles(stocks, start, end, model=model)
    ffn = formatFn(fn, format)
    writeFile(formatData(df, format), ffn, format)

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
        ffn = formatFn(fn, format)
        writeFile(formatData(df, format), ffn, format)

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


def getCandles(stocks, start, end, model=CandlesModel):
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

    start = start if isinstance(start, int) else dt2unix(start)
    end = end if isinstance(end, int) else dt2unix(end)
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
        start = dt2unix(start)
    fc = FinnCandles(stocks)
    fc.cycleStockCandles(start, model, latest, numcycles)


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


def startTickWS(stocks, fn='datafile', store=['csv'], delt=None):
    mws = MyWebSocket(stocks, fn, store=store, delt=delt)
    mws.start()
    return mws


def startTickWSKeepAlive(stocks, fn, store, delt=None, polltime=5):

    ws_thread = startTickWS(stocks, fn,  store, delt=delt)

    while True:
        cur = time.time()
        nexttime = cur + 240

        while time.time() < nexttime:
            if not ws_thread.is_alive():
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
    fn = formatFn(fn, format='json')
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
    while True:
        if not mws.is_alive():
            print('Websocket was stopped: restarting...')
            mws = MyWebSocket(stocks, fn, store=store, resample_td=resample_td, fq=fq, ffill=True)
            mws.start()
        time.sleep(polltime)
        print(' ** ')


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


def getGainersLosers(tickers, start, numstocks, model=CandlesModel):
    """
    Explanation
    ------------
    Filter the stocks in {tickers} for the largest price difference since the time {start}
    Parameters
    __________
    :params tickers: List<str>
    :params start: int: Unix time in seconds
    :params numstocks: The number of stocks to include in gainers and losers
    :return: (list<list>, list<list>): (gainers, losers): Each sub list is [stock, pricediff, percentagediff, firstprice, lastprice]
    """
    mc = ManageCandles(getSaConn(), model)
    return mc.filterGanersLosers(tickers, start, numstocks)


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
    createFirstQuote(timestamp, model, stocks)


def getFirstQuote(timestamp, wiggle=60, model=AllquotesModel):
    from quotedb.models.metamod import getSession
    s = getSession()
    fqs = Firstquote.availFirstQuotes(timestamp-wiggle, timestamp, s)

    if fqs:
        return fqs[-1]
    createFirstquote(timestamp, model=model)
    return Firstquote.getFirstquote(timestamp, s)


def getDeltaData(stocks, start, end, fq, model=AllquotesModel, format="df"):
    if fq is None:
        fq = getFirstQuote(start, model=model)
    mc = ManageCandles(getSaConn(), model)
    x = mc.getDeltaData(stocks, start, end, fq, format)
    return x


if __name__ == "__main__":
    pass
    pass
    # stocks = ["PDD", "ROKU", "ROST", "SBUX", "SIRI", "SWKS", "TCOM", "TXN", "VRSK", "VRSN", "VRTX", "WBA", "WDAY", "XEL", "XLNX", "ZM", ]
    # stocks = ['AAPL', 'AMZN', 'ROKU', 'GME', 'TSLA', 'BB', 'SQ', 'MU', 'BINANCE:BTCUSDT']
    # tz = 'US/Eastern'
    # start = pd.Timestamp("2021-3-11 11:30", tz=tz).tz_convert("UTC").replace(tzinfo=None)
    # end = pd.Timestamp("2021-3-11 12:00", tz=tz).tz_convert("UTC").replace(tzinfo=None)
    ########################################
    # from quotedb.sp500 import nasdaq100symbols
    # stocks = nasdaq100symbols
    # # # start = dt.datetime.utcnow()
    # # # start = dt.datetime.utcnow() - dt.timedelta(days=60)
    # start = dt.datetime(2021, 3, 21)
    # end = dt.datetime.utcnow()

    # # stocks = nasdaq100symbols
    # # # stocks = ['AAPL', 'SQ']
    # startCandles(stocks, start, latest=True)
    # # x = getCandles(stocks, start, end)
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
    # from quotedb.sp500 import nasdaq100symbols, getSymbols
    # from quotedb.sp500 import random50
    # from quotedb.utils.util import dt2unix_ny
    # stocks = None
    # start = dt2unix(dt.datetime.utcnow() - dt.timedelta(hours=3), unit='n')
    # end = dt2unix(dt.datetime.utcnow(), unit='n')
    # j = getPolyTrade(stocks, start, end)

    # import pandas as pd
    # fc = FinnCandles([])
    # stocks = getSymbols()
    # stocks = random50(numstocks=20)
    # # stocks.append('BINANCE:BTCUSDT')
    # # startdelt = dt.timedelta(days=75)

    # start = dt2unix_ny(dt.datetime(2021, 3, 30, 13, 45))
    # startdelt = dt.datetime(2021, 1, 1)
    # fn = 'visualizenow.json'
    # gltime = dt2unix(pd.Timestamp(2021,  3, 15, 12, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    # numrec = 10

    # getCurrentDataFile(stocks, start, fn, (start, numrec), model=AllquotesModel, format='visualize', bringtodate=False)

    ##############################################
    from quotedb.utils.util import dt2unix_ny
    stocks = list(random50(numstocks=7))
    stocks = ['CERN', 'CSCO', 'GILD', 'KDP', 'MAR', 'MU', 'AAPL']
    # stocks.append('BINANCE:BTCUSDT')
    # enable woking in off hours with some data from finnhub
    fn = f'{getCsvDirectory()}/ws_json.json'
    delt = dt.timedelta(seconds=0.25)
    format = 'db'

    # startTickWSKeepAlive(stocks, fn, store=[format])
    fq = dt2unix_ny(dt.datetime(2021, 4, 1, 15, 30))
    # stocks = ['AAPL', 'ROKU', 'TSLA', 'INTC', 'CCL', 'VIAC', 'ZKIN', 'AMD']
    startTickWS_SampleFill(stocks, fn, fq, delt=delt)

    ##############################################
    # import pandas as pd
    # from quotedb.sp500 import nasdaq100symbols, getSymbols
    # from pprint import pprint
    # start = dt2unix(pd.Timestamp(2021,  3, 29, 16, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    # end = dt2unix(dt.datetime.utcnow())
    # stocks = nasdaq100symbols
    # stocks = getSymbols()
    # numstocks = 10
    # model = AllquotesModel
    # gainers, losers = getGainersLosers(stocks, start, numstocks, model=model)
    # print("gainers", gainers)
    # print("losers", losers)

    # df = getCandles(stocks, start, end)
    # gainers, losers = localFilterStocks(df, stocks, (start, numstocks))

    # pprint(gainers)
    # print()
    # pprint(losers)
    # #####################################################
    # from quotedb.utils.util import dt2unix_ny
    # timestamp = dt2unix_ny(dt.datetime(2021, 3, 25, 3, 5, 0))
    # fqs = getFirstQuote(timestamp)
    # ##########################################################
    # from quotedb.getdata import getFirstQuote
    # from quotedb.models.allquotes_candlemodel import AllquotesModel
    # from quotedb.sp500 import getSymbols

    # timestamp = dt2unix_ny(dt.datetime(2021, 3, 25, 3, 5, 0))
    # fq = getFirstQuote(timestamp)
    # fq = None
    # start = dt2unix(pd.Timestamp(2021,  3, 29, 12, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    # end = dt2unix(dt.datetime.utcnow())
    # stocks = getQ100_Sp500()
    # stocks = getSymbols()

    # mc = ManageCandles(getSaConn(), AllquotesModel)
    # mc.getDeltaData(stocks, start, end, fq)

    # x = getDeltaData(stocks, start, end, fq)
