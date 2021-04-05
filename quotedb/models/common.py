"""
A collection of dtabase call functions that are common to one object or another
"""
import logging
import pandas as pd
from quotedb.models.metamod import getEngine, getSession
from quotedb.models.firstquotemodel import Firstquote, Firstquote_trades
from quotedb.sp500 import getSymbols


from sqlalchemy import text


def getFirstQuoteData(timestamp, tablename="allquotes", thestocks=None):
    """
    Explanation
    -----------
    Retrieve the data to create a firstquote from {tablename}. To work, the table must have
    the fields [stock, timestamp]. Works on allquotes by default. The result will be the candle
    data for {thestocks}. Each record will be for the max value <= timestamp
    """

    with getEngine().connect() as con:
        statement = text(f"""
            SELECT s1.*
                FROM allquotes s1
                    inner join  (SELECT *, max(timestamp) as mts
                        FROM {tablename}
                        WHERE timestamp <= {timestamp} GROUP BY stock) s2
                on s2.stock = s1.stock and s1.timestamp = s2.mts """)
        q = con.execute(statement).fetchall()
        # ############## pager ex
        # some_query = Query([TableBlaa])

        # query = some_query.limit(number_of_rows_per_page).offset(page_number*number_of_rows_per_page)
        # #  -- OR --
        # query = some_query.slice(page_number*number_of_rows_per_page, (page_number*number_of_rows_per_page)+number_of_rows_per_page)
        # current_pages_rows = session.execute(query).fetchall()

    # Remove duplicates
    stocks = []
    ret = []
    for qq in q:
        if qq.stock not in stocks:
            stocks.append(qq.stock)
            ret.append(qq)
    if not ret:
        return pd.DataFrame()

    df = pd.DataFrame(ret, columns=list(dict(ret[0]).keys()))
    if thestocks:
        df = df[df.stock.isin(thestocks)]
    return df


def createFirstQuote(timestamp, model, stocks="all", local=False, usecache=False):
    """
    Explanation
    -----------
    Create a new firstquote or update current. Try to guarantee that there will be an entry for
    every symbol in ALLSTOCKS as much as possible. Some of listed stocks get an illegal access
    error from finnhub. The data should have already beeen collected into the table represented
    by {model} before making this call.

    To collect the data (prior to creating firstquotes) use startCandles with a date that precedes
    timestamp by some amout of time. But in production, that call should run continuously

    Parameters
    ----------
    :params timestamp: int: unixtime
    :params stocks: union[str, list]: "all" will get candles from evey available US exchange. Otherwise
        send a list of the stockss to be included
    :params model: SqlAlchemy model: Currently either CandlesModel or AllqutoesModel. Will determine which table to use.
    :params local: bool: If false, save to the database
    :params usecache: bool: If true, use stored firstquote with the given timestamp. The stocks included in firstquote
        are not guaranteed by this library or the database.
    :params return: dict: {timestamp:timestamp, firstquotes_trades: list<Firstquote_trades>} The list is a local version
        without the database relation.
    """
    # plus = 60*60*3    # The number of seconds to pad the start time.
    stocks = stocks if isinstance(stocks, list) else getSymbols() if stocks == "all" else None
    if not stocks:
        logging.info("Invalid request in createFirstQuote")
        return None
    # mc = ManageCandles(getSaConn(), model)
    s = getSession()
    if usecache:
        fq = Firstquote.getFirstquote(timestamp, s)
        if fq:
            return fq
    candles = getFirstQuoteData(timestamp, model.__tablename__, thestocks=stocks)

    fqs = []
    for i, candle in candles.iterrows():
        fq = Firstquote_trades()
        fq.stock = candle.stock
        fq.close = candle.close
        if candle.timestamp < timestamp:
            fq.high = fq.low = fq.open = candle.close
            # Recored the volume if the time is within 1 minute
            fq.volume = 0 if timestamp - candle.timestamp > 60 else candle.volume
        else:
            fq.high = candle.high
            fq.low = candle.low
            fq.open = candle.open
            fq.volume = candle.volume
        fqs.append(fq)
    # fq = [Firstquote_trades(stock=d['stock'],
    #                         high=d['high'],
    #                         low=d['low'],
    #                         open=d['open'],
    #                         close=d['close'],
    #                         volume=d['volume']) for d in [dict(x) for x in candles]]
    if not local:
        Firstquote.addFirstquote(timestamp, fqs, s)
    fq = Firstquote(timestamp=timestamp, firstquote_trades=fqs)
    return fq


if __name__ == "__main__":
    # import datetime as dt
    # from quotedb.utils.util import dt2unix_ny
    # from quotedb.sp500 import nasdaq100symbols

    # stocks = nasdaq100symbols
    # d = dt2unix_ny(dt.datetime(2021, 3, 31, 12))
    # x = getFirstQuoteData(d, thestocks=stocks)

    # ###################################################
    import datetime as dt
    from quotedb.models.allquotes_candlemodel import AllquotesModel
    from quotedb.sp500 import nasdaq100symbols
    from quotedb.utils.util import dt2unix_ny

    timestamp = dt2unix_ny(dt.datetime(2021, 3, 31, 13, 0, 0))
    x = createFirstQuote(timestamp, model=AllquotesModel, stocks=nasdaq100symbols, local=True, usecache=True)
    print()
