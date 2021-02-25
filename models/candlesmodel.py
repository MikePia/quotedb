"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
import csv
import datetime as dt

from sqlalchemy import create_engine, Column, String, Integer, Float, func, distinct
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from stockdata.dbconnection import getSaConn, getCsvDirectory
from stockdata.sp500 import sp500symbols, nasdaq100symbols
from utils.util import dt2unix, unix2date

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
        for i, t in enumerate(arr):
            s.add(CandlesModel(
            symbol = symbol,
            close = t[0],
            high = t[1],
            low = t[2],
            open = t[3],
            time = t[4],
            vol = t[5]))
            if not i % 1000:
                s.commit()
                print(f'commited {i} records for symbol {symbol}')

        s.commit()

    @classmethod
    def getTimeRange(cls, start, end, engine):
        s = Session(bind=engine)
        q = s.query(CandlesModel).filter(CandlesModel.time>=start).filter(CandlesModel.time<=end).all()
        return q

    @classmethod
    def cleanDuplicatesFromResults(cls, symbol, arr, engine):
        """
        Remove results from arr that have a duplicate time and symbol in the db
        """
        s = Session(bind=engine)
        td = {t[4]:t for t in arr}
        times = set(list(td.keys()))
        q = s.query(CandlesModel.time).filter_by(symbol=symbol).filter(
                CandlesModel.time>=min(times)).filter(
                CandlesModel.time<=max(times)).order_by(CandlesModel.time).all()
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
        if tickers == None:
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
    def __init__(self, db, create=False):
        '''
        :params db: a SQLalchemy connection string. 
        '''
        self.db = db
        self.engine = create_engine(self.db)
        if create:
            self.createTables()

    def createTables(self):
        session = Session(bind=self.engine)
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
        prevtime  = q[0][0]
        for t in q:
            newtime = t[0]
            if newtime-prevtime > maxsize[0]:
                maxsize = (newtime-prevtime, newtime)
            prevtime=newtime
        print('max time is ', dt.timedelta(seconds=maxsize[0]))
        print('Occurs at ', unix2date(maxsize[1]))


if __name__ == '__main__':
    mc = ManageCandles(getSaConn(), True)
    mc.getLargestTimeGap('ZM')
    # mc.chooseFromReport(getCsvDirectory() + '/report.csv')
    # tickers = ['TXN', 'SNPS', 'SPLK', 'PTON', 'CMCSA', 'GOOGL']
    # mc.reportShape(tickers=mc.getQ100_Sp500())


    # #################################################################
    #  Create a classa or method to house jsoninfy Sqlalchemy results
    # import datetime as dt
    # import json
    # from stockdata.dbconnection import getCsvDirectory
    # print(getSaConn())
    # mk = ManageCandles(getSaConn(), True)
    # start = dt.datetime(2021,1,20,10,30,0)
    # end = dt.datetime(2021,1,20,16,30,0)
    # x = CandlesModel.getTimeRange(dt2unix(start), dt2unix(end), mk.engine)
    # xlist = [z.__dict__ for z in x]
    # for xd in xlist:
    #     del xd['_sa_instance_state']
    
    # j = json.dumps(xlist)
    # fn = getCsvDirectory() + f'file.json'
    # with open(fn, 'w', newline='') as f:
    #     f.write(j)
    

    # print()
    # print()
