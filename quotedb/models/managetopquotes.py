"""Database methods for the TopquotesModel class

ManageTopquotes implements a selection of methods from ManageCandles. It is an interface
pattern without external enforcement.
"""
import datetime as dt
import pandas as pd
from quotedb.models.common import createFirstQuote
from quotedb.models.firstquotemodel import Firstquote, Firstquote_trades
from quotedb.models.metamod import getSession, init, cleanup
from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.topquotes_candlemodel import TopquotesModel
from quotedb.utils.util import dt2unix, unix2date

from sqlalchemy import func, distinct


class ManageTopQuote:
    """
    Explanation
    -----------
    ManageTopQuote and ManageCandles share an interface only.

    Constructing ManageTopQuotes
    ----------------------------
    * User is responsible to create firstquote, know it exists, or indicate an empty table
    * If topquotes has any data, firstquote exists.
    * Create a firstquote by providing fq_time to this constructor/getManagedQuote
    * Indicate empty table be fq_time = -1
    """
    def __init__(self, stocks, model, create=False, fq_time=None):
        """
        Paramaters
        ----------
        :stocks: list[str]:
        """

        # ManageCandles.__init__(self, db, model, create=create)
        self.session = getSession()
        self.model = model
        if create:
            self.createTables()
        self.stocks = stocks
        if fq_time == -1:
            self.fq = None
            return
        if fq_time:
            # TODO: Not sure this is sequenced well but -- need to check existing fq against stocks
            # Problem is I don't think we can guarantee every stock ... how to determine an update ?
            self.updateFirstQuote(fq_time)
        self.fq = None
        fq = self.getFirstquote()
        if fq is None:
            if fq_time is None:
                raise ValueError('Topquote has no first quote and fq_time is None')
            self.fq = self.installFirstQuote(stocks, fq_time=fq_time)
        elif fq:
            self.fq = fq
        else:
            x = self.installFirstQuote(stocks, fq_time=fq_time)
            self.fq = x[1]

    def createTables(self):
        init()

    def installFirstQuote(self, stocks,  fq=None, fq_time=None, blitz=False):
        """
        Explanation
        -----------
        Install a first quote as the earliest data in the table. If data already already exists

        Parameters:
        :params stocks: list:
        :params fq:  tuple(dict, int): ({stock}: [price, volume], timestamp)  (*not yet used by anything)
        :params fq_time: int: unixtime
        :params blitz: bool: If True, remove the current contents of the the table if a new firstquote is installed
        """
        # TODO: This is not currently changable from arguments. The Topquote-Firstquote derives from allquotes here
        getFromModel = AllquotesModel
        s = self.session
        if fq is not None:
            assert isinstance(fq, Firstquote)
            # if fq is the same timestamp as current fq, update the stocks if necessary
            mint = s.query(func.min(TopquotesModel.timestamp)).one_or_none()
            if fq and fq.timestamp == mint[0]:
                curfq = self.getFirstquote()
                fqstocks = [x.stock for x in fq.firstquote_trades]
                curstocks = [x.stock for x in curfq.firstquote_trades]
                missing = set(fqstocks) - set(curstocks)
                if not missing:
                    self.fq = fq
            elif fq:
                # Requesting an update of the installed Firstquote
                raise NotImplementedError("Should we completely blitz topquotes table here?")

        else:

            # TODO: Ensure the time corresponds with a minute marker
            # Truncating datetime to minute. This really needs to be tested with against finnhub data
            d = unix2date(fq_time)
            fqt = dt2unix(dt.datetime(d.year, d.month, d.day, d.hour, d.minute))
            trades = createFirstQuote(fqt, stocks=stocks, model=getFromModel,  local=True)
            installedfq = False
            for trade in trades.firstquote_trades:
                installedfq = True
                t = TopquotesModel(stock=trade.stock,
                                   close=trade.close,
                                   high=trade.high,
                                   low=trade.low,
                                   open=trade.open,
                                   timestamp=trades.timestamp,
                                   volume=trade.volume,
                                   delta_t=0,
                                   delta_p=0.0)
                s.add(t)
            if installedfq:
                s.commit()
                cleanup()
                init()

    def updateFirstQuote(self, timestamp):
        """
        Paramaters
        ----------
        :params return: tuple(dict, int) or None

        Programming Notes
        -----------------
        Put off the implementation until this concept is proved workable
        """
        s = getSession()
        q = s.query(TopquotesModel).limit(10).all()
        if not q:
            self.installFirstQuote(self.stocks, fq_time=timestamp)

    # @override
    def addCandles(self, stock, df, session):
        '''
        Explanation
        -----------
        Search for duplicates in the database. An entry with the same timestamp and stock is a duplicate,
        Add the rest to the database

        Paramaters
        ----------
        :stocks: list:
        :df: DataFrame or list[c, h, l, o, t, v]

        '''
        if len(df) == 0:
            return
        if self.fq is None:
            self.fq = self.getFirstquote()
        assert self.fq is not None
        if isinstance(df, list) and isinstance(stock, str):
            df = pd.DataFrame(df, columns=['close', 'high', 'low', 'open', 'timestamp', 'volume'])
        df['stock'] = stock

        retries = 5

        s = getSession()
        q = s.query(self.model.timestamp, self.model.stock).filter(
                self.model.timestamp >= min(df.timestamp)).filter(
                self.model.timestamp <= max(df.timestamp)).all()
        q = {(x.timestamp, x.stock): ['something'] for x in q}

        while retries > 0:
            try:
                init()
                dupcount = 0
                counter = 0
                for i in range(len(df)):
                    if q.get((int(df.iloc[i].timestamp), df.iloc[i].stock)):
                        dupcount += 1
                        if not dupcount % 1000:
                            print(f'Found {dupcount} duplicates')
                        continue
                    this_fq = [x for x in self.fq.firstquote_trades if x.stock == stock]

                    s.add(self.model(
                        stock=df.iloc[i].stock,
                        close=df.iloc[i].close,
                        high=df.iloc[i].high,
                        low=df.iloc[i].low,
                        open=df.iloc[i].open,
                        timestamp=df.iloc[i].timestamp,
                        volume=df.iloc[i].volume,
                        delta_p=(df.iloc[i]['close'] - this_fq[0].close) / this_fq[0].close,
                        delta_t=df.iloc[i]['timestamp'] - self.fq.timestamp,
                        ))
                    counter += 1
                    if not counter % 1000:
                        s.commit()
                        print(f'commited {counter} records for stock {stock}')

                print(f'Found {dupcount} duplicates')
                print(f'commited {len(df)-dupcount} records for stock {stock}')
                s.commit()
                retries = 0
            except Exception as ex:
                print(ex, f'Retry #{retries}')
                retries -= 1
                continue
            finally:
                cleanup()

    def getFirstquote(self):
        """
        Explanation
        -----------
        Retrieve records for stocks that have the min(timestamp) in the table.
        The earliest timestamp in this table is firstquote. It is guaranteed(?) to have an entry for
        each stock in self.stocks (at least at creation, update is lacking here.). Retrieve all
        firstquote data as dict, with {stock} as key and
        [price, volume] as value

        Programming Note
        ----------------
        * This is an unusual pattern. Dynamic data in a rdb is verbotten. But will go to lengths
        * to guarantee the state of the table.
            * The firstquote needs to be earliest timestamp
            * Every relavant stock must have an entry and no stocks besides those in firstquote are allowed
            * Updating involves either changing every entry or starting from scratch
        * It is a work in progress, these guarantees are not yet there, and the table will
        function with careful oversight until they are

        Return
        ----------
        :return: Firstquote
        """
        s = getSession()
        if self.fq:
            return self.fq
        mint = s.query(func.min(TopquotesModel.timestamp)).one_or_none()
        # q = s.query(TopquotesModel).order_by(TopquotesModel.timestamp)
        mint = mint[0] if mint else None
        if not mint:
            return None
            # {d.stock: [d.close, d.volume] for d in fq}
        fqdata = s.query(TopquotesModel).filter_by(timestamp=mint).all()
        fqtrades = [Firstquote_trades(stock=x.stock,
                                      open=x.open,
                                      high=x.high,
                                      low=x.low,
                                      close=x.close,
                                      volume=x.volume) for x in fqdata]

        fq = Firstquote(timestamp=mint, firstquote_trades=fqtrades)

        return fq

    # ###################################################
    # Place these in a common db interface location (ABC?)
    def getMaxTimeForEachTicker(self, tickers=None):
        maxdict = dict()
        if tickers is None:
            tickers = self.getTickers(self.session)
        for tick in tickers:
            t = self.getMaxTime(tick, self.session)
            if t:
                maxdict[tick] = t
        return maxdict

    def getTickers(self):
        s = self.session

        tickers = s.query(distinct(self.model.stock)).all()
        tickers = [x[0] for x in tickers]
        return tickers

    def getMaxTime(self, ticker, session):
        s = session
        q = s.query(func.max(self.model.timestamp)).filter_by(stock=ticker).one_or_none()
        return q[0]

    def getMinTime(self, ticker, session):
        s = session
        q = s.query(func.min(self.model.timestamp)).filter_by(stock=ticker).one_or_none()
        return q[0]

    def getTimeRange(self, stock, start, end):
        """
        Paramaters
        ----------
        :params stock: list<str>:
        :params start: int: Unix time
        :params end: int Unix time
        :params return: List<model>: list of candles as sa types
        """
        s = getSession()
        q = s.query(self.model).filter_by(stock=stock).filter(self.model.timestamp >= start).filter(self.model.timestamp <= end).all()
        return q


if __name__ == '__main__':
    #########################################
    # from quotedb.dbconnection import getSaConn
    # from quotedb.sp500 import nasdaq100symbols
    # stocks = nasdaq100symbols
    # stocks = nasdaq100symbols
    # mtq = ManageTopQuote(stocks, getSaConn(), TopquotesModel)
    # d = dt.datetime(2021, 3, 25, 15, 30)
    # mtq.installFirstQuote(stocks, fq_time=d)
    ###########################################
    from quotedb.sp500 import nasdaq100symbols
    from quotedb.utils.util import dt2unix_ny

    stocks = nasdaq100symbols
    fq_time = dt2unix_ny(dt.datetime(2021, 3, 30, 12, 0, 0))
    mtq = ManageTopQuote(stocks, AllquotesModel, fq_time=-1)
    end = dt2unix_ny(dt.datetime.utcnow())
    # mtq = ManageTopQuote(stocks, TopquotesModel, fq_time=fq_time)

    df = mtq.getTimeRangeMultiple(mtq.stocks, fq_time, end, model=AllquotesModel)
    mtq.addCandles(None, df, None)
