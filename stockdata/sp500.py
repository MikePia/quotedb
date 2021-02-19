import pandas as pd

tables=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
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



if __name__ == '__main__':
    print(sp500symbols[:10])
    print(nasdaq100symbols[:10])