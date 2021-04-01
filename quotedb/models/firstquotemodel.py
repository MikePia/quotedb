"""
firstquote is one to many relationship. The primary table firstquote and secondary firstquote_trades.
When an entry is created, this code in ManageFirstquote.addFirstquote(). The supporting code will
attempt to guarantee to have every stock accounted for each firstquote entry. (Would be bad form to
call the REST api from this class)
An update method will determine if allstocks has changed and request new data if it has.

The data used to gather this data may come from finnhub our our database. This class won't care(but the initial
implementation will all come from finnhub)
"""

from quotedb.models.metamod import Base, getEngine
from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text


class Firstquote(Base):
    __tablename__ = 'firstquote'
    id = Column(Integer, primary_key=True)
    timestamp = Column(Integer, nullable=False, unique=True)

    def __repr__(self):
        return f'firstquote: {self.timestamp}'

    @classmethod
    def addFirstquote(cls, timestamp, candles, session):
        """
        Explanation
        -----------
        Create a new firstquote or add to the firstquote_trades array or do nothing

        Paramaters
        ----------
        :params timestamp: int. unix time in seconds
        :params candles: list<Firstquote_trades>:
        :params session: sqlalchemy session
        """
        s = session
        q = Firstquote.getFirstquote(timestamp, s)
        if not q:
            s.add(Firstquote(timestamp=timestamp, firstquote_trades=candles))
            s.commit()
        else:
            addthese = []
            stocks = [t.stock for t in q.firstquote_trades]
            for candle in candles:
                if candle.stock not in stocks:
                    addthese.append(candle)
            if addthese:
                q.firststock_trades.extend(addthese)
                s.add(q)
                s.commit()

    @classmethod
    def getNumStocks(cls, id, stocklist=False):

        with getEngine().connect() as con:
            if not stocklist:
                statement = text(f"""
                    SELECT count(distinct ft.stock), f.timestamp
                    FROM firstquote_trades ft, firstquote f WHERE ft.firstquote_id = {id} """)
            else:
                statement = text(f"""
                    SELECT distinct ft.stock,  f.timestamp
                    FROM firstquote_trades ft, firstquote f WHERE ft.firstquote_id = {id} """)
            q = con.execute(statement).fetchall()

        return q

    @classmethod
    def availFirstQuotes(cls, start, end, session):
        '''
        Explanation
        -----------
        Retrieve the available first qotes between start and end

        Return
        ------
        :return: list<int>: A list of timestamps in the databases between start and end
        '''
        s = session
        q = s.query(Firstquote).filter(Firstquote.timestamp >= start).filter(Firstquote.timestamp <= end).all()
        return q

    @classmethod
    def getFirstquote(cls, timestamp, session):
        s = session
        q = s.query(Firstquote).filter(Firstquote.timestamp == timestamp).one_or_none()
        return q

    @classmethod
    def deleteFirstquote(cls, timestamp, session):
        s = session
        fq = Firstquote.getFirstquote(timestamp, session)
        if fq:
            s.delete(fq)

    @classmethod
    def getFirstquoteTimestamps(cls, session, low=None, high=None):
        s = session
        if not low and not high:
            q = s.query(Firstquote).all()
            return q


class Firstquote_trades(Base):
    __tablename__ = "firstquote_trades"
    id = Column(Integer, primary_key=True)
    stock = Column(String(8), nullable=False)

    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    firstquote_id = Column(Integer, ForeignKey('firstquote.id'))

    firstquote = relationship("Firstquote", back_populates="firstquote_trades")
    UniqueConstraint('firstquote_id', 'stock', name='unique_fk_stock')  # Not working

    def __repr__(self):
        return f"<firstquote_trade({self.stocks}, {self.close})>"


Firstquote.firstquote_trades = relationship("Firstquote_trades", back_populates="firstquote")

# The call UniqueConstraing not working. Here is the sql to do it
# ALTER TABLE `stockdb`.`firstquote_trades` ADD UNIQUE `stock_firstquoteid` (`stock`, `firstquote_id`);


# Base.metadata.create_all(engine)
if __name__ == '__main__':
    # init()
    # Base.metadata.create_all(engine)
    # #####################################################
    from quotedb.models.metamod import getSession
    # print(Firstquote.getFirstquoteTimestamps(getSession()))

    # #####################################################
    print(Firstquote.getNumStocks(10))
    print()
    # print(Firstquote.getNumStocks(10, stocklist=True)[:20])
    for fq in Firstquote.availFirstQuotes(0, 999999999999999, getSession()):
        print(fq.id, fq.timestamp, 'numstocks:', len(fq.firstquote_trades))
