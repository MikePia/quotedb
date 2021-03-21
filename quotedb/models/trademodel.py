"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
import csv
import datetime as dt

from sqlalchemy import create_engine, Column, String, Integer, Float, distinct
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from quotedb.dbconnection import getSaConn, getCsvDirectory
from quotedb.utils.util import unix2date, dt2unix

Base = declarative_base()
Session = sessionmaker()


class TradeModel(Base):
    """
    Hlds data generated for stock, forex and crypto. Adjustihng for possible long names to 24 chars
    Also volumes can be fractions for crypto at least, type is Float here.
    Condition will be stored and interpreted in ManageTrade class
    """
    __tablename__ = "trade"
    id = Column(Integer, primary_key=True)
    # These can be stock, forex or crypto and names can be something like BINANCE:BTCUSDT
    symbol = Column(String(24), nullable=False, index=True)
    price = Column(Float, nullable=False)
    time = Column(Float, nullable=False, index=True)
    volume = Column(Float, nullable=False)
    condition = Column(Integer)

    @classmethod
    def addTrades(cls, arr, engine):
        '''
        [p, t, v, c]
        '''
        s = Session(bind=engine)
        # arr = TradesModel.cleanDuplicatesFromResults(symbol, arr, engine)
        for i, t in enumerate(arr, 1):
            condition = ','.join([str(x) for x in t['c']]) if t['c'] else ''
            s.add(TradeModel(
                  symbol=t['s'],
                  price=t['p'],
                  time=t['t'],
                  volume=t['v'],
                  condition=condition))
            if not i % 50:
                s.commit()
                print(f'commited {i} records to trade table')
        print(f'Commited {len(arr)} records to trade table')
        s.commit()

    @classmethod
    def getTimeRangeMultiple(cls, symbols, start, end, session):
        """
        :params symbols: arr<str>
        :params start: int. Unix time in milliseconds
        :params end: int. Unix time in milliseconds
        """
        s = session

        q = s.query(TradeModel).filter(
            TradeModel.time >= start).filter(
            TradeModel.time <= end).filter(
            TradeModel.symbol.in_(symbols)).order_by(
            TradeModel.time.asc(), TradeModel.symbol.asc()).all()
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
            tickers = s.query(distinct(TradeModel.symbol)).all()
            tickers = [x[0] for x in tickers]
        # There is probably some cool way to get all the data in one sql statement. THIS COULD BE VERY TIME CONSUMING
        for tick in tickers[::-1]:
            # I think first thing is just change this to a sql execute without ORM (did not help much)

            with engine.connect() as con:
                statement = text(f"""SELECT min(time), max(time), count(time) FROM trade WHERE symbol="{tick}";""")
                q = con.execute(statement).fetchall()
                if q[0][2] == 0:
                    d[tick] = [q[0][0], q[0][1], q[0][2]]
                    print(f'{tick}: {q[0][0]}: {q[0][1]}: {q[0][2]} ')
                else:
                    d[tick] = [unix2date(q[0][0]), unix2date(q[0][1]), q[0][2]]
                    print(f'{tick}: {unix2date(q[0][0])}: {unix2date(q[0][1])}: {q[0][2]} ')
        return d


class ManageTrade:
    def __init__(self, db, create=False):
        '''
        :params db: a SQLalchemy connection string.
        '''
        self.db = db
        self.engine = create_engine(self.db)
        if create:
            self.createTables()

    def createTables(self):
        self.session = Session(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def reportShape(self, tickers=None):
        """
        Could analyze differences in db holdings here, For now just print it out
        """
        d = TradeModel.getReport(self.engine, tickers)

        fn = getCsvDirectory() + "/tradereport.csv"
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


if __name__ == '__main__':
    mt = ManageTrade(getSaConn(), create=True)
    stocks = ['AAPL', 'AMZN', 'ROKU', 'GME', 'TSLA', 'BB', 'SQ', 'MU', 'BINANCE:BTCUSDT']
    start = dt2unix(dt.datetime(2021, 2, 27), unit='m')
    end = dt2unix(dt.datetime(2021, 3, 8), unit='m')
    x = TradeModel.getTimeRangeMultiple(stocks, start, end, mt.session)
    print(len(x))
