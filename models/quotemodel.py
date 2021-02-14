"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
from sqlalchemy import create_engine, Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from stockdata.dbconnection import getSaConn

Base = declarative_base()
Session = sessionmaker()

class QuotesModel(Base):
    __tablename__ = "quotes"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(8), nullable=False)
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
            symbol = t,
            close = arr[t]['c'],
            high = arr[t]['h'],
            low = arr[t]['l'],
            open = arr[t]['o'],
            price = arr[t]['pc'],
            time = arr[t]['t'],
            vol = arr[t]['v']
        ) for t in arr]
        s.add_all(x)
        s.commit()

    def removeQuotesByDate(cls, unixdate, engine):
        s = Session(bind=engine)
        q = s.query(QuotesModel).filter_by(unixdate<time).all()




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
        session = Session(bind=self.engine)
        Base.metadata.create_all(self.engine)


if __name__ == '__main__':
    print(getSaConn())
    mk = ManageQuotes(getSaConn(), True)
