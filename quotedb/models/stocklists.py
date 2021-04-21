"""Keep tables of nasdaq100, sp500, and allsymbols with methods."""
import sys
from quotedb.models import metamod as mm

from sqlalchemy import Column, String, Float, BigInteger, Integer


class Nasdaq100(mm.Base):
    __tablename__ = "nasdaq100"
    stock = Column(String(8), primary_key=True)

    @classmethod
    def updateNasdaq(cls, stocks):
        s = mm.getSession()
        if not stocks or len(stocks) < 100:
            raise ValueError("Invalid list for nasdaq 100")
            sys.exit()

        statement = 'delete from nasdaq100'
        mm.getEngine().execute(statement)

        mm.cleanup()
        mm.init()
        s.add_all([Nasdaq100(stock=x) for x in stocks])
        s.commit()

    @classmethod
    def getNasdaq100(cls):
        s = mm.getSession()
        q = s.query(Nasdaq100.stock).all()
        return [x[0] for x in q]


class Sp500(mm.Base):
    __tablename__ = "sp500"
    stock = Column(String(8), primary_key=True)

    @classmethod
    def updateSp500(cls, stocks):
        s = mm.getSession()
        if not stocks or len(stocks) < 500:
            raise ValueError("Invalid list for sp500")
            sys.exit()

        statement = 'delete from sp500'
        mm.getEngine().execute(statement)

        mm.cleanup()
        mm.init()
        s.add_all([Sp500(stock=x) for x in stocks])
        s.commit()

    @classmethod
    def getSp500(cls):
        s = mm.getSession()
        q = s.query(Sp500.stock).all()
        return [x[0] for x in q]


class allsymbols(mm.Base):
    __tablename__ = "allsymbols"
    stock = Column(String(8), primary_key=True)
    name = Column(String(200))
    lastsale = Column(Float)
    netchange = Column(Float)
    pctchange = Column(Float)
    marketcap = Column(BigInteger)
    Country = Column(String(50))
    ipoyear = Column(Integer)
    volume = Column(Integer)
    sector = Column(String(100))
    industry = Column(String(100))


if __name__ == '__main__':
    mm.init()
    # from quotedb.sp500 import nasdaq100symbols, sp500symbols
    # Nasdaq100.updateNasdaq(nasdaq100symbols)
    # Sp500.updateSp500(sp500symbols)
    n = Nasdaq100.getNasdaq100()
    print(n[:10])
    print()
    s = Sp500.getSp500()
    print(s[:10])
