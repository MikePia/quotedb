"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
import csv
import datetime as dt
import pandas as pd

from .metamod import (init, cleanup, getSession,
                      Base, Session)
from sqlalchemy import create_engine, Column, String, Integer, Float, distinct, desc, func
from sqlalchemy.sql import text

from quotedb.dbconnection import getSaConn, getCsvDirectory
from quotedb.polygon.polytrade import isMarketOpen
from quotedb.utils.util import dt2unix, unix2date, resample, unix2date_ny
# from quotedb.sp500 import getQ100_Sp500


class CandlesModel(Base):
    __tablename__ = "candles"
    id = Column(Integer, primary_key=True)
    stock = Column(String(8), nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    timestamp = Column(Integer, nullable=False, index=True)
    volume = Column(Integer, nullable=False)

    @classmethod
    def addCandles(cls, stock, arr, session):
        '''
        [c, h, l, o, t, v]
        '''
        retries = 5
        while retries > 0:
            try:
                init()
                s = getSession()
                arr = CandlesModel.cleanDuplicatesFromResults(stock, arr, session)
                if len(arr) == 0:
                    return
                for i, t in enumerate(arr, start=1):
                    s.add(CandlesModel(
                        stock=stock,
                        close=t[0],
                        high=t[1],
                        low=t[2],
                        open=t[3],
                        timestamp=t[4],
                        volume=t[5]))
                    if not i % 1000:
                        s.commit()
                        print(f'commited {i} records for stock {stock}')

                print(f'commited {len(arr)} records for stock {stock}')
                s.commit()
                retries = 0
            except Exception as ex:
                print(ex, f'Retry #{retries}')
                retries -= 1
                continue
            finally:
                cleanup()

    @classmethod
    def getTimeRange(cls, stock, start, end, session):
        s = session
        q = s.query(CandlesModel).filter_by(stock=stock).filter(CandlesModel.timestamp >= start).filter(CandlesModel.timestamp <= end).all()
        return q

    @classmethod
    def getTimeRangeMultiple(cls, symbols, start, end, session):
        """
        Query candles for all stocks that have times between start and end
        """
        s = session

        q = s.query(CandlesModel).filter(
            CandlesModel.timestamp >= start).filter(
            CandlesModel.timestamp <= end).filter(
            CandlesModel.stock.in_(symbols)).order_by(
            CandlesModel.timestamp.asc(), CandlesModel.stock.asc()).all()
        return q

    @classmethod
    def getTimeRangeMultipleVpts_slow(cls, symbols, start, end, session):
        s = session

        q = s.query(CandlesModel.stock, CandlesModel.close.label("price"), CandlesModel.timestamp, CandlesModel.volume).filter(
            CandlesModel.timestamp >= start).filter(
                CandlesModel.timestamp <= end).filter(
                CandlesModel.stock.in_(symbols)).order_by(
                CandlesModel.timestamp.asc(), CandlesModel.stock.asc())
        q = q.all()
        return [r._asdict() for r in q]

    @classmethod
    def getTimeRangeMultipleVpts(cls, symbols, start, end, session):
        print(f'Getting candles for {len(symbols)} stocks between {unix2date_ny(start)} and {unix2date_ny(end)} NY time')
        s = session
        q = s.query(CandlesModel.stock, CandlesModel.close, CandlesModel.timestamp, CandlesModel.volume).filter(
            CandlesModel.timestamp >= start).filter(CandlesModel.timestamp <= end).all()
        df = pd.DataFrame([(d.stock, d.close, d.timestamp, d.volume) for d in q], columns=['stock', 'price', 'timestamp', 'volume'])
        df = df[df.stock.isin(symbols)]
        return df

    @classmethod
    def getTimeRangePlus(cls, stock, start, end, session):
        '''
        Retrieve the timestamp range but guarantee that the first timestamp has either a current value
        or a previous value
        '''
        data = CandlesModel.getTimeRange(stock, start-(60*30), end, session)
        if not data:
            return []
        if data[0].timestamp <= start:
            return data
        s = session
        q = s.query(CandlesModel).filter(CandlesModel.timestamp < start).order_by(desc(CandlesModel.timestamp)).first()
        if q:
            data.insert(0, q)
        else:
            # TODO
            print('No current or earlier data for start')
            raise ValueError('Programmers Exception, Here is the case to deal with')
        return data

    @classmethod
    def getMaxTime(cls, ticker, session):
        s = session
        q = s.query(func.max(CandlesModel.timestamp)).filter_by(stock=ticker).one_or_none()
        return q[0]

    @classmethod
    def getTickers(cls, session):
        s = session

        tickers = s.query(distinct(CandlesModel.stock)).all()
        tickers = [x[0] for x in tickers]
        return tickers

    @classmethod
    def cleanDuplicatesFromResults(cls, stock, arr, session):
        """
        Remove results from arr that have a duplicate timestamp and stock in the db
        """
        s = session
        td = {t[4]: t for t in arr}
        times = set(list(td.keys()))
        q = s.query(CandlesModel.timestamp).filter_by(stock=stock).filter(
                CandlesModel.timestamp >= min(times)).filter(
                CandlesModel.timestamp <= max(times)).order_by(CandlesModel.timestamp).all()
        times2 = set([x[0] for x in q])
        for tt in (times & times2):
            del td[tt]
        if len(times) > len(td):
            print(f'Found {len(times) - len(td)} duplicates')
        return list(td.values())

    @classmethod
    def getReport(cls, session, tickers=None):
        """
        Currently developers tool only, it's too slow. It's Query problems and maybe some SA tweaking
        But it is really useful even looking over it with eyes can see potential missing data based on
        beginning dates. It will need to be automated. 100 stocks is very different than 8000
        Get the min and max dates of tickers.
        :params tickers: list, if tickers is None, report on every ticker in the db
        :return: {ticker:[mindate<int>, maxdate<int>, numrec:<int>], ...}
        """
        d = {}
        s = session
        if tickers is None:
            tickers = s.query(distinct(CandlesModel.stock)).all()
            tickers = [x[0] for x in tickers]
        # There is probably some cool way to get all the data in one sql statement. THIS COULD BE VERY TIME CONSUMING
        for tick in tickers[::-1]:
            # I think first thing is just change this to a sql execute without ORM (did not help much)
            # select min(timestamp), max(timestamp), count(timestamp) from candles where stock="SIRI";

            with engine.connect() as con:
                statement = text(f"""SELECT min(timestamp), max(timestamp), count(timestamp) FROM candles WHERE stock="{tick}";""")
                q = con.execute(statement).fetchall()
                if q[0][2] == 0:
                    d[tick] = [q[0][0], q[0][1], q[0][2]]
                    print(f'{tick}: {q[0][0]}: {q[0][1]}: {q[0][2]} ')
                else:
                    d[tick] = [unix2date(q[0][0]), unix2date(q[0][1]), q[0][2]]
                    print(f'{tick}: {unix2date(q[0][0])}: {unix2date(q[0][1])}: {q[0][2]} ')
        return d

    @classmethod
    def printLatestTimes(cls, stocks, session):
        from quotedb.utils.util import unix2date
        for stock in stocks:
            t = CandlesModel.getMaxTime(stock, session)
            print(unix2date(t, unit='s').strftime("%A %B, %d %H:%M%S"))
        print(isMarketOpen())


if __name__ == '__main__':
    # getRange()
    # mc = ManageCandles(getSaConn(), True)
    # mc = ManageCandles(getSaConn())
    # CandlesModel.printLatestTimes(nasdaq100symbols, mc.session)
    # mc.getLargestTimeGap('ZM')
    # mc.chooseFromReport(getCsvDirectory() + '/report.csv')
    # tickers = ['TXN', 'SNPS', 'SPLK', 'PTON', 'CMCSA', 'GOOGL']
    # mc.reportShape(tickers=mc.getQ100_Sp500())

    # #################################################################
    # from pprint import pprint
    # mc = ManageCandles(getSaConn())
    # start = dt2unix(pd.Timestamp(2021,  3, 16, 12, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    # print(pd.Timestamp(start, unit='s'))
    # # stocks = ['AAPL', 'SQ']
    # stocks = getQ100_Sp500()
    # gainers, losers = mc.filterGanersLosers(stocks, start, 10)
    #####################################
    # start = 1609463187
    # end = 1615943202
    # print(unix2date(start))
    # print(unix2date(end))
    # stocks = getQ100_Sp500()

    # mc = ManageCandles(getSaConn())
    # df = CandlesModel.getTimeRangeMultipleVpts(stocks, start, end, mc.session)
    #############################################
    mc = ManageCandles(getSaConn(), create=True)
    start = dt2unix(pd.Timestamp(2021,  3, 12, 12, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    end = dt2unix(pd.Timestamp.utcnow().replace(tzinfo=None))
    x = mc.getFilledData('AAPL', start, end)
