"""
Using the finnhub candles endpoint cycle through stocks. When the requirements become
clearer, add commandline args with date shortcuts and possibility of reading a file for
tickers


"""
if __name__ == '__main__':
    import pandas as pd
    from quotedb.getdata import startCandles

    # from quotedb.models.candlesmodel import CandlesModel
    from quotedb.models.allquotes_candlemodel import AllquotesModel
    # from quotedb.models.topquotes_candlemodel import TopquotesModel
    from quotedb.sp500 import getSymbols, nasdaq100symbols
    from quotedb.utils.util import dt2unix

    # stocks = sorted(nasdaq100symbols)
    stocks = getSymbols()
    stocks = ['AAPL', 'TSLA', 'ROKU']
    stocks = nasdaq100symbols

    start = dt2unix(pd.Timestamp(2021, 4, 1, 12, 36).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    # model = TopquotesModel
    model = AllquotesModel
    # model = CandlesModel
    startCandles(stocks, start, model, latest=True)
