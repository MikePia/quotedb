import datetime as dt
from quotedb.utils import util
from quotedb import sp500
from quotedb.getdata import getJustGainersLosers
from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.candlesmodel import CandlesModel
from quotedb.finnhub.finncandles import FinnCandles


def taskit():
    start = util.dt2unix_ny(dt.datetime(2021, 5, 17, 9, 30))
    numrecs = 35
    end = util.dt2unix(dt.datetime.utcnow())
    stocks = sp500.getSymbols()
    latest = False
    numcycles = 10
    stocks = getJustGainersLosers(start, end, stocks, numrecs, AllquotesModel, local=False)
    fc = FinnCandles(stocks)
    fc.cycleStockCandles(start=start, model=CandlesModel, latest=latest, numcycles=numcycles)


if __name__ == '__main__':
    taskit()
