"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
from sqlalchemy import create_engine, Column, String, Integer, Float, func, distinct
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from stockdata.dbconnection import getSaConn

Base = declarative_base()
Session = sessionmaker()


class QuotesModel(Base):
    __tablename__ = "quotes"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(8), nullable=False, index=True)
    price = Column(Float, nullable=False)
    time = Column(Integer, nullable=False)

    @classmethod
    def addQuotes(cls, arr, session):
        '''
        Note that o, h, l, and pc refer to the previous day and we don't save them
        :params: t json results from quote/us?
        '''
        s = session
        # arr = QuotesModel.cleanDuplicatesFromResult(arr, session)
        x = [QuotesModel(
            symbol=t['s'],
            price=t['c'],
            time=t['t']
        ) for t in arr]
        s.add_all(x)
        s.commit()

    @classmethod
    def getTickers(cls, session):
        s = session

        tickers = s.query(distinct(QuotesModel.symbol)).all()
        tickers = [x[0] for x in tickers]
        return tickers
        
    @classmethod
    def getMaxTime(cls, ticker, session):
        s = session
        q = s.query(func.max(QuotesModel.time)).filter_by(symbol=ticker). one_or_none()
        return q[0]

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


if __name__ == '__main__':
    print(getSaConn())
    mk = ManageQuotes(getSaConn(), True)
    # start = dt2unix(dt.datetime(2021, 2, 11, 9, 30))
    # end = dt2unix(dt.datetime(2021, 2, 12, 10, 30))
    # stocks = nasdaq100symbols
    # x = QuotesModel.getTimeRangeMultiple(stocks, start, end, mk.session)
    # print()
