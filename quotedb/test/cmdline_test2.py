import datetime as dt
from quotedb.getdata import getCandles
from quotedb.sp500 import nasdaq100symbols


def dostuff():

    stocks = nasdaq100symbols
    start = dt.datetime(2021, 3, 21)
    end = dt.datetime.utcnow()

    x = getCandles(stocks, start, end)

    print(f'Retrieved {len(x)} items')
    return f'Retrieved {len(x)} items'


dostuff()

# callum switched
# dostuff()
# off 20210324_21115
# calling dostuff() in shiny app instead
