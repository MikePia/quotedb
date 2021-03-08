import datetime as dt
import json
import pandas as pd

from stockdata.dbconnection import getSaConn

from models.candlesmodel import ManageCandles, CandlesModel
from models.finntickmodel import ManageFinnTick, FinnTickModel
from models.trademodel import ManageTrade, TradeModel

from stockdata.finnhub.stockquote import StockQuote
from stockdata.finnhub.trades import MyWebSocket
from stockdata.sp500 import nasdaq100symbols
from utils.util import dt2unix


def getCandles(stocks, start, end, api='fh', format='json'):
    # from stockdata.dbconnection import getCsvDirectory
    print(getSaConn())
    mk = ManageCandles(getSaConn(), True)
    x = CandlesModel.getTimeRangeMultiple(stocks, dt2unix(start), dt2unix(end), mk.session)
    if not x:
        return {}
    xlist = [z.__dict__ for z in x]
    for xd in xlist:
        del xd['_sa_instance_state']

    j = json.dumps(xlist)
    return j


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


def startTickWS(stocks):
    MyWebSocket(stocks)


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
    # sq = StockQuote(random50(numstocks=5), begtime)
    # sq = StockQuote(['AAPL', 'TSLA'], begtime, limit=25000)

    # print(sq._StockQuote__getTicks("SQ", begtime))
    delt = pd.Timedelta(hours=2)
    # sq._StockQuote__getTicksOnDay('SQ', startat=delt)
    sq.cycleStockTicks(startat=delt)


if __name__ == "__main__":
    # stocks = ["PDD", "ROKU", "ROST", "SBUX", "SIRI", "SWKS", "TCOM", "TXN", "VRSK", "VRSN", "VRTX", "WBA", "WDAY", "XEL", "XLNX", "ZM", ]
    # stocks = ['AAPL', 'AMZN', 'ROKU', 'GME', 'TSLA', 'BB', 'SQ', 'MU', 'BINANCE:BTCUSDT']
    start = dt.datetime(2021, 3, 5)
    end = dt.datetime(2021, 3, 8)
    # x = getCandles(stocks, start, end)
    # x = getTicks(stocks, start, end)
    # MyWebSocket(stocks)
    stocks = nasdaq100symbols
    # dadate = dt.date(2021, 3, 5)
    # delt = pd.Timedelta(minutes=30)
    # startLastXTicks(stocks, dadate, delt)
    x = getTicksREST(stocks, start, end)
    print()
    print()
