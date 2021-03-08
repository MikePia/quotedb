"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
import datetime as dt

from sqlalchemy import create_engine, Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from stockdata.dbconnection import getSaConn
from stockdata.sp500 import nasdaq100symbols
from utils.util import dt2unix

Base = declarative_base()
Session = sessionmaker()


class QuotesModel(Base):
    __tablename__ = "quotes"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(8), nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    time = Column(Integer, nullable=False)
    vol = Column(Integer, nullable=False)

    @classmethod
    def addQuotes(cls, arr, engine):
        '''
        :params: t json results from quote/us?
        '''
        s = Session(bind=engine)
        x = [QuotesModel(
            symbol=t,
            close=arr[t]['c'],
            high=arr[t]['h'],
            low=arr[t]['l'],
            open=arr[t]['o'],
            price=arr[t]['pc'],
            time=arr[t]['t'],
            vol=arr[t]['v']
        ) for t in arr]
        s.add_all(x)
        s.commit()

    @classmethod
    def getTimeRangeMultiple(cls, symbols, start, end, session):
        """
        :params symbols: arr<str>
        :params start: int. Unix time in milliseconds
        :params end: int. Unix time in milliseconds
        """
        s = session

        q = s.query(QuotesModel).filter(
            QuotesModel.time >= start).filter(
            QuotesModel.time <= end).filter(
            QuotesModel.symbol.in_(symbols)).order_by(
            QuotesModel.time.asc(), QuotesModel.symbol.asc()).all()
        return q

    # def removeQuotesByDate(cls, unixdate, engine):
    #     s = Session(bind=engine)
    #     q = s.query(QuotesModel).filter_by(unixdate < time).all()


class ManageQuotes:
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


if __name__ == '__main__':
    print(getSaConn())
    mk = ManageQuotes(getSaConn(), True)
    start = dt2unix(dt.datetime(2021, 2, 11, 9, 30))
    end = dt2unix(dt.datetime(2021, 2, 12, 10, 30))
    stocks = nasdaq100symbols
    x = QuotesModel.getTimeRangeMultiple(stocks, start, end, mk.session)
    print()
