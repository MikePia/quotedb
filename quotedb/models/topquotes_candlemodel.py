from quotedb.models.managecandles import ManageCandles
from quotedb.models.metamod import (init, Base)
from sqlalchemy import Column, String, Integer, Float



class TopquotesModel(Base):
    '''
    Inheritance of CandleModel would seem the right move but its not due to how
    the concrete table inheritance works in sqlalchemy. Instead, the best performing
    solution requires seperate models for the various candle tables.
    '''
    __tablename__ = "topquotes"

    id = Column(Integer, primary_key=True)
    stock = Column(String(8), nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    timestamp = Column(Integer, nullable=False, index=True)
    volume = Column(Integer, nullable=False)


class ManageTopQuote(ManageCandles):
    def __init__(self)


if __name__ == '__main__':
    init()
    print()
