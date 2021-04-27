import os
import random

import pandas as pd


def getSymbolsFromQFile():

    # The file was downlaoded from here. It is probably possible to to automated acces to an ftp file that is updated nightly
    # url = 'https://www.nasdaq.com/market-activity/stocks/screener'

    fn = ''
    thisdir = os.path.split(__file__)[0]
    files = os.listdir(thisdir)
    for file in files:
        if file.startswith("nasdaq_screener") and file > fn:
            fn = file
    if fn:
        fn = os.path.join(thisdir, fn)
        assert os.path.exists(fn), f"File not found {fn}"
        stocks = pd.read_csv(os.path.join(thisdir, fn))
        return list(stocks.Symbol)

    raise ValueError('Stock symbols were not found')


def getSymbols():
    """
    Explanation
    -----------
    Gets the symbols for the US market. The source is nasdaq.com. Secondary offering names are
    changed to the version finnhub accepts.

    Return
    ------
    List of ~ 7.5k symbols

    """
    from quotedb.finnhub.finncandles import FinnCandles
    sym = getSymbolsFromQFile()
    fc = FinnCandles([])
    sym2 = fc.getSymbols()
    len(sym2)
    second = [x for x in sym if x.find("^") > 0]
    second2 = [x for x in sym2 if x.find(".") > 0]
    second_a = [x for x in sym if x.find("/") > 0]
    second2
    symbols = set(sym) - set(second)
    for stock in set([x.split('^')[0] for x in second]):
        st = stock + '^'
        sl = len(st)
        st2 = stock + '.'
        s1 = sorted([x for x in second if x[:sl] == st])
        s2 = sorted([x for x in second2 if x[:sl] == st2])
        for s in s1:
            sp = s.split('^')
            tick = f'{sp[0]}.PR{sp[1]}'
            if tick in s2:
                symbols.add(tick)

    for stock in set([x.split('/')[0] for x in second_a]):
        st = stock + '/'
        sl = len(st)
        st2 = stock + '.'
        s1 = sorted([x for x in second_a if x[:sl] == st])
        s2 = sorted([x for x in second2 if x[:sl] == st2])
        for s in s1:
            sp = s.split('/')
            tick = f'{sp[0]}.{sp[1]}'
            if tick in s2:
                symbols.add(tick)
    return sorted(symbols)


try:
    tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    if not tables or len(tables) == 0:
        raise ValueError
except Exception:
    from quotedb.models import stocklists as sl
    print('wiki pedia thing failed, falling back to stored data')
    sp500symbols = sl.Sp500.getSp500()
    tables = None


sp500 = None
if tables:
    sp500 = tables[0]
if sp500 is not None:
    sp500symbols = list(sp500['Symbol'])

try:
    tables = pd.read_html('https://en.wikipedia.org/wiki/NASDAQ-100')
    nasdaq = None
    if not tables or len(tables) == 0:
        raise ValueError
    for table in tables:
        if 'Company' in table.keys() and 'Ticker' in table.keys():
            nasdaq = table
            break
except Exception:
    from quotedb.models import stocklists as sl
    print('wiki pedia thing failed, falling back to stored data')
    nasdaq100symbols = sl.Nasdaq100.getNasdaq100()
    tables, nasdaq = None, None


if nasdaq is not None:
    nasdaq100symbols = list(nasdaq['Ticker'])


def random50(stocks=nasdaq100symbols, numstocks=50, exclude=[]):
    '''
    get 50 stocks. Note stocks should be len 100 or greater
    '''
    results = set()
    while len(results) < numstocks:
        n = random.randint(0, len(stocks)-1)
        if not stocks[n] in exclude:
            results.add(stocks[n])
    return list(results)


def getQ100_Sp500():
    st = set(sp500symbols).union(set(nasdaq100symbols))
    st = sorted(list(st))
    return st


if __name__ == '__main__':
    # print(sp500symbols[:10])
    # print(nasdaq100symbols[:10])

    # x = random50(nasdaq100symbols)
    x = getQ100_Sp500()
    print(len(x))
    x = getSymbols()
    print(len(x))
    # https://ns31235677.ip-151-106-32.eu:8443/login_up.php?success_redirect_url=%2F
