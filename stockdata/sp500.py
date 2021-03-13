import random
import pandas as pd


tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
sp500 = None
if tables:
    sp500 = tables[0]
if sp500 is not None:
    sp500symbols = list(sp500['Symbol'])

tables = pd.read_html('https://en.wikipedia.org/wiki/NASDAQ-100')
nasdaq = None
for table in tables:
    if 'Company' in table.keys() and 'Ticker' in table.keys():
        nasdaq = table
        break

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
    return results


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
