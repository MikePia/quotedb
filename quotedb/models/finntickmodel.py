"""
Represents a table finntick created to store tick data from finnhub. No indication that
volume can be fractional. (not available crypto of forex (I think)). The time element is
receiv ed in microseconds (the trade data is nanoseconds and candles are seconds)
"""
import csv

from sqlalchemy import create_engine, Column, String, Integer, Float, func, distinct, desc, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from quotedb.dbconnection import getSaConn, getCsvDirectory
from quotedb.utils.util import unix2date

Base = declarative_base()
Session = sessionmaker()


class FinnTickModel(Base):
    __tablename__ = "finntick"
    id = Column(Integer, primary_key=True)
    # These can be stock, forex or crypto and names can be something like BINANCE:BTCUSDT
    symbol = Column(String(24), nullable=False)
    price = Column(Float, nullable=False)
    time_ms = Column(BigInteger, nullable=False)
    volume = Column(Integer, nullable=False)
    condition = Column(String(24))

    # @classmethod
    # def addTicksFromDf(cls, symbol, df, engine):
    #     '''
    #     :params df: Dataframe of results from polygon trade endpoint. But the data has likely
    #     been resampled, rendering the condition field of little use. Will be left Null (Default)
    #     '''
    #     s = Session(bind=engine)
    #     for i, row in enumerate(df.itertuples(), start=1):
    #         s.add(FinnTickModel(
    #             symbol=symbol,
    #             price=row.price,
    #             time_ms=row.time,
    #             volume=row.volume))
    #         if not i % 10000:
    #             s.commit()
    #             print(f'commited {i} records to {symbol}')
    #     print(f'Commited {len(df)} rows for {symbol}\n')
    #     s.commit()

    @classmethod
    def addTicks(cls, symbol, arr, session):
        '''
        Use this to process the json results from finnhub tick endpoint
        :params arr: [price, time, volume, [conditions]]
        '''
        s = session
        # arr = TradesModel.cleanDuplicatesFromResults(symbol, arr, engine)
        for i, t in enumerate(arr, 1):
            # Save a list of integersa as a string for the condition field
            if t[3]:
                condition = ','.join([str(x) for x in t[3]])
            else:
                condition = ''
            s.add(FinnTickModel(
                  symbol=symbol,
                  price=t[0],
                  time_ms=t[1],
                  volume=t[2],
                  condition=condition))
            if not i % 5000:
                s.commit()
                print(f'commited {i} records for symbol {symbol}')
        print(f'commited {len(arr)} records for symbol {symbol}\n')
        s.commit()

    @classmethod
    def getTickers(cls, session):
        s = session

        tickers = s.query(distinct(FinnTickModel.symbol)).all()
        tickers = [x[0] for x in tickers]
        return tickers

    @classmethod
    def getTimeRangeMultiple(cls, symbols, start, end, session):
        """
        :params symbols: arr<str>
        :params start: int. Unix time in milliseconds
        :params end: int. Unix time in milliseconds
        """
        s = session

        q = s.query(FinnTickModel).filter(
            FinnTickModel.time_ms >= start).filter(
            FinnTickModel.time_ms <= end).filter(
            FinnTickModel.symbol.in_(symbols)).order_by(
            FinnTickModel.time_ms.asc(), FinnTickModel.symbol.asc()).all()
        return q

    @classmethod
    def getTimes(cls, session):
        s = session
        times = s.query(FinnTickModel.time_ms).all()
        times = [x[0] for x in times]
        return times

    @classmethod
    def tail(cls, ticker, session, numrec=10):
        s = session
        q = s.query(FinnTickModel).order_by(desc(FinnTickModel.time_ms)).limit(numrec).all()
        return q

    @classmethod
    def getReport(cls, engine, tickers=None):
        """
        Currently developers tool only, it's too slow. It's Query problems and maybe some SA tweaking
        But it is really useful even looking over it with eyes can see potential missing data based on
        beginning dates. It will need to be automated. 100 stocks is very different than 8000
        Get the min and max dates of tickers.
        :params tickers: list, if tickers is None, report on every ticker in the db
        :return: {ticker:[mindate<int>, maxdate<int>, numrec:<int>], ...}
        """
        d = {}
        s = Session(bind=engine)
        if tickers is None:
            tickers = s.query(distinct(FinnTickModel.symbol)).all()
            tickers = [x[0] for x in tickers]
        # There is probably some cool way to get all the data in one sql statement. THIS COULD BE VERY TIME CONSUMING
        for tick in tickers[::-1]:
            # I think first thing is just change this to a sql execute without ORM (did not help much)
            # select min(time_ms), max(time_ms), count(time_ms) from candles where symbol="SIRI";

            with engine.connect() as con:
                statement = text(f"""SELECT min(time_ms), max(time_ms), count(time_ms) FROM finntick WHERE symbol="{tick}";""")
                q = con.execute(statement).fetchall()
                if q[0][2] == 0:
                    d[tick] = [q[0][0], q[0][1], q[0][2]]
                    print(f'{tick}: {q[0][0]}: {q[0][1]}: {q[0][2]} ')
                else:
                    d[tick] = [unix2date(q[0][0]), unix2date(q[0][1]), q[0][2]]
                    print(f'{tick}: {unix2date(q[0][0])}: {unix2date(q[0][1])}: {q[0][2]} ')
        return d

    @classmethod
    def getMaxTime(cls, ticker, session):
        s = session
        q = s.query(func.max(FinnTickModel.time_ms)).filter_by(symbol=ticker).one_or_none()
        return q[0]

    @classmethod
    def getMinTime(cls, ticker, session):
        s = session
        q = s.query(func.min(FinnTickModel.time_ms)).filter_by(symbol=ticker).one_or_none()
        return q[0]

    @classmethod
    def selectTicker(cls, ticker, session):
        s = session
        q = s.query(FinnTickModel).filter_by(symbol=ticker).all()
        return q


class ManageFinnTick:
    engine = None
    session = None

    def __init__(self, db, create=False):
        '''
        :params db: a SQLalchemy connection string.
        '''
        self.db = db
        self.engine = create_engine(self.db)
        self.session = Session(bind=self.engine)
        if create:
            self.createTables()

    def createTables(self):
        # self.session = Session(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def reportShape(self, tickers=None):
        """
        Could analyze differences in db holdings here, For now just print it out
        """
        d = FinnTickModel.getReport(self.engine, tickers)

        fn = getCsvDirectory() + "/finntickreport.csv"
        with open(fn, 'a', newline='') as file:
            for k, v, in d.items():
                csv_file = csv.writer(file)
                csv_file.writerow([k, v[0], v[1], v[2]])
                # print(f'{tick}: {unix2date(q[0][0])}: {unix2date(q[0][1])}: {q[0][2]} ')
                print(f'{k}: {v[0]}: {v[1]}: {v[2]} ')

    def chooseFromReport(self, fn, numRecords=None, minDate=None):
        csvfile = []
        with open(fn, 'r') as file:
            reader = csv.reader(file, dialect="excel")
            for row in reader:
                csvfile.append(row)

        if numRecords is not None:
            return [x[0] for x in csvfile if int(x[3]) <= numRecords]

    def getMaxTimeForEachTicker(self, tickers=None):
        maxdict = dict()
        if tickers is None:
            tickers = FinnTickModel.getTickers(self.engine)
        for tick in tickers:
            t = FinnTickModel.getMaxTime(tick, self.session)
            if t:
                maxdict[tick] = t
        return maxdict


if __name__ == '__main__':
    import pandas as pd
    mft = ManageFinnTick(getSaConn(), True)
    print(pd.Timestamp(FinnTickModel.getMaxTime("SQ", mft.session), unit="ms"))
    print(pd.Timestamp(FinnTickModel.getMinTime("SQ", mft.session), unit="ms"))
