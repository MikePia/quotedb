"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
from sqlalchemy import create_engine, Column, String, Integer, Float, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from stockdata.dbconnection import getSaConn
from utils.util import dt2unix, unix2date

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
            if not i % 200:
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
        return list(td.values())




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
    # Create a classa or method to house jsoninfy Sqlalchemy results
    import datetime as dt
    import json
    from stockdata.dbconnection import getCsvDirectory
    print(getSaConn())
    mk = ManageCandles(getSaConn(), True)
    start = dt.datetime(2021,1,20,10,30,0)
    end = dt.datetime(2021,1,20,16,30,0)
    x = CandlesModel.getTimeRange(dt2unix(start), dt2unix(end), mk.engine)
    xlist = [z.__dict__ for z in x]
    for xd in xlist:
        del xd['_sa_instance_state']
    
    j = json.dumps(xlist)
    fn = getCsvDirectory() + f'file.json'
    with open(fn, 'w', newline='') as f:
        f.write(j)
    

    print()
    print()
