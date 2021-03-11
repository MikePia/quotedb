"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
import csv
import datetime as dt
import pandas as pd

from sqlalchemy import create_engine, Column, String, Integer, Float, distinct, desc, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from stockdata.dbconnection import getSaConn, getCsvDirectory
from stockdata.sp500 import sp500symbols, nasdaq100symbols
from utils.util import dt2unix, unix2date, resample

Base = declarative_base()
Session = sessionmaker()


class CandlesModel(Base):
    __tablename__ = "candles"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(8), nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    time = Column(Integer, nullable=False, index=True)
    vol = Column(Integer, nullable=False)

    @classmethod
    def addCandles(cls, symbol, arr, engine):
        '''
        [c, h, l, o, t, v]
        '''
        s = Session(bind=engine)
        arr = CandlesModel.cleanDuplicatesFromResults(symbol, arr, engine)
        for i, t in enumerate(arr, start=1):
            s.add(CandlesModel(
                  symbol=symbol,
                  close=t[0],
                  high=t[1],
                  low=t[2],
                  open=t[3],
                  time=t[4],
                  vol=t[5]))
            if not i % 1000:
                s.commit()
                print(f'commited {i} records for symbol {symbol}')

        print(f'commited {len(arr)} records for symbol {symbol}')
        s.commit()

    @classmethod
    def getTimeRange(cls, symbol, start, end, engine):
        s = Session(bind=engine)
        q = CandlesModel.filter_by(symbol=symbol).filter(CandlesModel.time >= start).filter(CandlesModel.time <= end).all()
        return q

    @classmethod
    def getTimeRangeMultiple(cls, symbols, start, end, session):
        s = session
        
        q = s.query(CandlesModel).filter(CandlesModel.time >= start).filter(CandlesModel.time <= end).filter(CandlesModel.symbol.in_(symbols)).order_by(CandlesModel.time.asc(), CandlesModel.symbol.asc()).all()
        return q

    @classmethod
    def getTimeRangePlus(cls, symbol, start, end, engine):
        '''
        Retrieve the time range but guarantee that the first time has either a current value
        or a previous value
        '''
        data = CandlesModel.getTimeRange(symbol, start-(60*30), end, engine)
        if data and data[0].time > start:
            s = Session(bind=engine)
            q = s.query(CandlesModel).filter(CandlesModel.time < start).order_by(desc(CandlesModel.time)).first()
            if q:
                data.insert(0, q)
            else:
                # TODO
                print('No current or earlier data for start')
                raise ValueError('Programmers Exception, Here is the case to deal with')
        elif data:
            # TODO
            print('No current or earlier data for start')
            raise ValueError('Programmers Exception, Here is the case to deal with')
        return data

    @classmethod
    def getMaxTime(cls, ticker, session):
        s = session
        q = s.query(func.max(CandlesModel.time)).filter_by(symbol=ticker).one_or_none()
        return q[0]

    @classmethod
    def getTickers(cls, session):
        s = session

        tickers = s.query(distinct(CandlesModel.symbol)).all()
        tickers = [x[0] for x in tickers]
        return tickers

    @classmethod
    def cleanDuplicatesFromResults(cls, symbol, arr, engine):
        """
        Remove results from arr that have a duplicate time and symbol in the db
        """
        s = Session(bind=engine)
        td = {t[4]: t for t in arr}
        times = set(list(td.keys()))
        q = s.query(CandlesModel.time).filter_by(symbol=symbol).filter(
                CandlesModel.time >= min(times)).filter(
                CandlesModel.time <= max(times)).order_by(CandlesModel.time).all()
        times2 = set([x[0] for x in q])
        for tt in (times & times2):
            del td[tt]
        if len(times) > len(td):
            print(f'Found {len(times) - len(td)} duplicates')
        return list(td.values())

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
            tickers = s.query(distinct(CandlesModel.symbol)).all()
            tickers = [x[0] for x in tickers]
        # There is probably some cool way to get all the data in one sql statement. THIS COULD BE VERY TIME CONSUMING
        for tick in tickers[::-1]:
            # I think first thing is just change this to a sql execute without ORM (did not help much)
            # select min(time), max(time), count(time) from candles where symbol="SIRI";

            with engine.connect() as con:
                statement = text(f"""SELECT min(time), max(time), count(time) FROM candles WHERE symbol="{tick}";""")
                q = con.execute(statement).fetchall()
                # q = s.query(func.min(CandlesModel.time), func.max(CandlesModel.time), func.count(CandlesModel.time)).filter_by(symbol=tick).all()
                if q[0][2] == 0:
                    d[tick] = [q[0][0], q[0][1], q[0][2]]
                    print(f'{tick}: {q[0][0]}: {q[0][1]}: {q[0][2]} ')
                else:
                    d[tick] = [unix2date(q[0][0]), unix2date(q[0][1]), q[0][2]]
                    print(f'{tick}: {unix2date(q[0][0])}: {unix2date(q[0][1])}: {q[0][2]} ')
        return d


class ManageCandles:
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
        self.session = Session(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def reportShape(self, tickers=None):
        """
        Could analyze differences in db holdings here, For now just print it out
        """
        d = CandlesModel.getReport(self.engine, tickers)

        fn = getCsvDirectory() + "/report.csv"
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

    def getQ100_Sp500(self):
        st = set(sp500symbols).union(set(nasdaq100symbols))
        st = sorted(list(st))
        return st

    def getLargestTimeGap(self, ticker):
        s = Session(bind=self.engine)
        q = s.query(CandlesModel.time).filter_by(symbol="ZM").order_by(CandlesModel.time).all()
        maxsize = (0, 0)
        prevtime = q[0][0]
        for t in q:
            newtime = t[0]
            if newtime-prevtime > maxsize[0]:
                maxsize = (newtime-prevtime, newtime)
            prevtime = newtime
        print('max time is ', dt.timedelta(seconds=maxsize[0]))
        print('Occurs at ', unix2date(maxsize[1]))

    def getFilledData(self, symbol, begin, end, format='json'):
        '''
        The atomic method to Query db for data between begin and end and return one record
        for each minute. Note that this is atomic because it is intended for data on a single day. Use
        getFilledDataDays for multiday
        '''
        data = CandlesModel.getTimeRangePlus(symbol, begin, end, self.engine)
        if not data:
            return None
        datadict = [x.__dict__ for x in data]
        cols = ['open', 'high', 'low', 'close', 'time', 'vol']
        datadict = pd.DataFrame.from_dict(datadict)[cols]
        data = resample(datadict, 'time', dt.timedelta(seconds=60))
        data.close = data.close.fillna(method='ffill')
        data.open = data.open.fillna(data.close)
        data.high = data.high.fillna(data.close)
        data.low = data.low.fillna(data.close)
        data.vol = data.vol.fillna(0)
        if format == 'csv':
            return data
        return data.to_json()

    def getFilledDataDays(self, symbol, startdate, enddate, policy='extended', custom=None, format='json'):
        '''
        :params startdate: <date> Get data beginning on this day
        :params enddate: <date> Get data ending on this day (inclusive)
        :params policy: One of [market, extended, 24_7]
            'market' will return data between 9:30 and 16:00
            'extended' will return data between 7:00 and 19:00
            '24_7' will return the entire day  (Actually 24-5, no weekends)
        :params custom: (begin<time>, end<time>) Overrides policy to return using data between begin and end
        '''
        delt = dt.timedelta(days=1)
        current = startdate
        if custom:
            begin = custom[0]
            end = custom[1]
        else:
            if policy == 'extended':
                begin = dt.time(7, 0, 0)
                end = dt.time(19, 0, 0)
            elif policy == 'market':
                begin = dt.time(9, 30, 0)
                end = dt.time(16, 0, 0)
            elif policy == '24_7':
                begin = dt.time(0, 0, 0)
                end = dt.time(23, 59, 0)
        df = None
        while current <= enddate:
            if current.weekday() < 5:
                curstart = int(pd.Timestamp(current.year, current.month, current.day, begin.hour, begin.minute, begin.second).timestamp())
                curend = int(pd.Timestamp(current.year, current.month, current.day, end.hour, end.minute, end.second).timestamp())
                newdf = self.getFilledData(symbol, curstart, curend, format='csv')
                if df is None and newdf is not None:
                    df = newdf
                elif newdf is not None:
                    df = df.append(newdf, ignore_index=True)
            current += delt
        if format == 'csv':
            return df
        return df.to_json() if df is not None else df

    def getMaxTimeForEachTicker(self, tickers=None):
        maxdict = dict()
        if tickers is None:
            tickers = CandlesModel.getTickers(self.session)
        for tick in tickers:
            t = CandlesModel.getMaxTime(tick, self.session)
            if t:
                maxdict[tick] = t
        return maxdict


def getRange():
    d1 = dt.date(2021, 1, 23)
    d2 = dt.date(2021, 2, 10)
    mc = ManageCandles(getSaConn())
    symbol = 'ZM'
    mc.getFilledDataDays(symbol, d1, d2)
    print()


if __name__ == '__main__':
    # getRange()
    mc = ManageCandles(getSaConn(), True)
    # mc.getLargestTimeGap('ZM')
    # mc.chooseFromReport(getCsvDirectory() + '/report.csv')
    # tickers = ['TXN', 'SNPS', 'SPLK', 'PTON', 'CMCSA', 'GOOGL']
    # mc.reportShape(tickers=mc.getQ100_Sp500())

    # #################################################################
    # #  Create a classa or method to house jsoninfy Sqlalchemy results
    # import json
    # # from stockdata.dbconnection import getCsvDirectory
    # print(getSaConn())
    # mk = ManageCandles(getSaConn(), True)
    # start = dt.datetime(2021, 1, 20, 10, 30, 0)
    # end = dt.datetime(2021, 1, 20, 16, 30, 0)
    # x = CandlesModel.getTimeRange('ROKU', dt2unix(start), dt2unix(end), mk.engine)
    # xlist = [z.__dict__ for z in x]
    # for xd in xlist:
    #     del xd['_sa_instance_state']

    # j = json.dumps(xlist)
    # fn = getCsvDirectory() + f'file.json'
    # with open(fn, 'w', newline='') as f:
    #     f.write(j)

    # # print()
    # # print()
