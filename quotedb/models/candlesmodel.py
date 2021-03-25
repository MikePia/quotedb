"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
from quotedb.models.metamod import Base, init
from sqlalchemy import Column, String, Integer, Float


class CandlesModel(Base):
    __tablename__ = "candles"
    id = Column(Integer, primary_key=True)
    stock = Column(String(8), nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    timestamp = Column(Integer, nullable=False, index=True)
    volume = Column(Integer, nullable=False)


if __name__ == '__main__':
    init()
