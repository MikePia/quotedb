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

class CandlesModel(Base):
    __tablename__ = "candles"
    id = Column(Integer, primary_key=True)
    symbol = Column(String(8), nullable=False)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    time = Column(Integer, nullable=False)
    vol = Column(Integer, nullable=False)

    @classmethod
    def addCandles(cls, arr, engine):
        '''
        [symbol, c, h, l, o, t, v]
        '''
        s = Session(bind=engine)
        for i, t in enumerate(arr):
            s.add(CandlesModel(
            symbol = t[0],
            close = t[1],
            high = t[2],
            low = t[3],
            open = t[4],
            time = t[5],
            vol = t[6]))
            if not i % 200:
                s.commit()
                print(f'commited {i} records')
        s.commit()




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


if __name__ == '__main__':
    print(getSaConn())
    mk = ManageCandles(getSaConn(), True)
