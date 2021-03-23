from quotedb.models.metamod import (init, cleanup, getSession,
                      Base)
from sqlalchemy import Column, String, Integer, Float


class Allquotes(Base):
    '''
    Inheritance of CandleModel would seem the right move but its not due to how
    the concrete table inheritance works in sqlalchemy. Instead, the best performing
    solution requires seperate models for the various candle tables.
    '''
    __tablename__ = "allquotes"

    id = Column(Integer, primary_key=True)
    stock = Column(String(8), nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    timestamp = Column(Integer, nullable=False, index=True)
    volume = Column(Integer, nullable=False)

    @classmethod
    def addCandles(cls, stock, arr, session):
        '''
        [c, h, l, o, t, v]
        '''
        retries = 5
        while retries > 0:
            try:
                init()
                s = getSession()
                arr = Allquotes.cleanDuplicatesFromResults(stock, arr, session)
                if len(arr) == 0:
                    return
                for i, t in enumerate(arr, start=1):
                    s.add(Allquotes(
                        stock=stock,
                        close=t[0],
                        high=t[1],
                        low=t[2],
                        open=t[3],
                        timestamp=t[4],
                        volume=t[5]))
                    if not i % 1000:
                        s.commit()
                        print(f'commited {i} records for stock {stock}')

                print(f'commited {len(arr)} records for stock {stock}')
                s.commit()
                retries = 0
            except Exception as ex:
                print(ex, f'Retry #{retries}')
                retries -= 1
                continue
            finally:
                cleanup()


if __name__ == '__main__':
    init()
    print()
