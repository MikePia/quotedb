"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
import csv
import datetime as dt
import numpy as np
import pandas as pd

from sqlalchemy import create_engine, Column, String, Integer, Float, func, distinct, desc, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from stockdata.dbconnection import getSaConn, getCsvDirectory
from stockdata.sp500 import sp500symbols, nasdaq100symbols
from utils.util import dt2unix, unix2date, resample

Base = declarative_base()
Session = sessionmaker()

class PolyTradeModel(Base):
    """
    Hlds data generated for stock, forex and crypto. Adjustihng for possible long names to 24 chars
    Also volumes can be fractions for crypto at least, type is Float here.
    Condition will be stored and interpreted in ManageTrade class
    """
    __tablename__ = "polytrade"
    id = Column(Integer, primary_key=True)
    # These can be stock, forex or crypto and names can be something like BINANCE:BTCUSDT
    symbol = Column(String(24), nullable=False)
    price = Column(Float, nullable=False)
    time_ns = Column(BigInteger, nullable=False)
    volume = Column(Float, nullable=False)
    condition = Column(String(24))

    @classmethod
    def addTradesFromDf(cls, symbol, df, engine):
        '''
        :params df: Dataframe of results from polygon trade endpoint. But the data has likely 
        been resampled, rendering the condition field of little use. Will be left Null (Default)
        '''
        s = Session(bind=engine)
        for i, row in enumerate(df.itertuples(), start=1):
            s.add(PolyTradeModel(
                symbol = symbol,
                price = row.price,
                time_ns = row.time,
                volume = row.volume ))
            if not i % 10000:
                s.commit()
                print(f'commited {i} records to {symbol}')
        print(f'Commited {len(df)} rows for {symbol}\n')
        s.commit()

    @classmethod
    def addTrades(cls,symbol, arr, engine):
        '''
        Use this to process the json results from polygon trade endpoint
        :params arr: A list of dict. The result of calling results.json()
        '''
        s = Session(bind=engine)
        # arr = TradesModel.cleanDuplicatesFromResults(symbol, arr, engine)
        for i, t in enumerate(arr, 1):
            # Save a list of integersa as a string for the condition field
            if t.get('c'):
                condition = ','.join([str(x) for x in t['c']])
            else:
                condition = ''
            s.add(PolyTradeModel(
            symbol = symbol,
            price = t['p'],
            time_ns = t['t'],
            volume = t['s'],
            condition = condition
            
            ))
            if not i % 5000:
                s.commit()
                print(f'commited {i} records to polytrade table')

        s.commit()

    @classmethod
    def getTickers(cls, engine):
        s = Session(bind=engine)
        
        tickers = s.query(distinct(PolyTradeModel.symbol)).all()
        tickers = [x[0] for x in tickers]
        return tickers

    @classmethod
    def getTimes(cls, engine):
        s = Session(bind=engine)
        times = s.query(PolyTradeModel.time_ns).all()
        times = [x[0] for x in times]
        return times

    @classmethod
    def tail(cls, ticker, engine, numrec=10):
        s = Session(bind=engine)
        q =  s.query(PolyTradeModel).order_by(desc(PolyTradeModel.time_ns)).limit(numrec).all()
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
        if tickers == None:
            tickers = s.query(distinct(PolyTradeModel.symbol)).all()
            tickers = [x[0] for x in tickers]
        # There is probably some cool way to get all the data in one sql statement. THIS COULD BE VERY TIME CONSUMING
        for tick in tickers[::-1]:
            # I think first thing is just change this to a sql execute without ORM (did not help much)
            # select min(time_ns), max(time_ns), count(time_ns) from candles where symbol="SIRI";
          
            with engine.connect() as con:    
                statement = text(f"""SELECT min(time_ns), max(time_ns), count(time_ns) FROM polytrade WHERE symbol="{tick}";""")
                q = con.execute(statement).fetchall()
                # q = s.query(func.min(CandlesModel.time_ns), func.max(CandlesModel.time_ns), func.count(CandlesModel.time_ns)).filter_by(symbol=tick).all()
                if q[0][2] == 0:
                    d[tick] = [q[0][0], q[0][1], q[0][2]]
                    print(f'{tick}: {q[0][0]}: {q[0][1]}: {q[0][2]} ')
                else:
                    d[tick] = [unix2date(q[0][0]), unix2date(q[0][1]), q[0][2]]
                    print(f'{tick}: {unix2date(q[0][0])}: {unix2date(q[0][1])}: {q[0][2]} ')
        return d


class ManagePolyTrade:
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
        d = PolyTradeModel.getReport(self.engine, tickers)

        fn = getCsvDirectory() + "/polytradereport.csv"
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
    mt = ManagePolyTrade(getSaConn(), create=True)
