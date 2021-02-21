import csv
import json
import logging
import requests
import time
import datetime as dt

from models.quotemodel import QuotesModel, ManageQuotes
from models.candlesmodel import CandlesModel, ManageCandles
from qexceptions.qexception import InvalidServerResponseException
from stockdata.sp500 import sp500symbols, nasdaq100symbols
from stockdata.dbconnection import getFhToken, getSaConn, getCsvDirectory
from utils.util import dt2unix, unix2date


class StockQuote:

    BASEURL = "https://finnhub.io/api/v1/"
    QUOTES = BASEURL + "quote/us?"
    CANDLES = BASEURL+ "stock/candle?"
    SINGLEQUOTE = BASEURL+ "quote?"
    HEADERS = {'Content-Type': 'application/json', 'X-Finnhub-Token': getFhToken()}

    def getSingleQuote(self, symbol):
        
        params = {}
        params['symbol'] = symbol
        response = requests.get(self.SINGLEQUOTE, params=params, headers=self.HEADERS)
        status = response.status_code
        if status != 200:
            raise InvalidServerResponseException(f'Server returned status {status}:{response.message}')
        return response.json()

    def runSingleQuotes(self, symbols):
        q = []
        for i, symbol in enumerate(symbols[100:200]):
            # if not i % 10:
            #     print(f'retrieving {symbol}, number {i}')
            try:
                q.append(self.getSingleQuote(symbol))
            except InvalidServerResponseException as ex:
                print(ex)
                continue


    def runquotes(self):
        base = self.QUOTES
        params = {}
        # params['token'] = 'c0b4p7748v6sc0gs4oq0'
        response = requests.get(self.QUOTES, headers=self.HEADERS)
        # response = requests.get(self.QUOTES, params=params)
        status = response.status_code
        # if status != 200:
        #     raise 
        return response.json()


    # TODO: use Twitsted or Chronus
    def getQuotes(self, start:dt.datetime, stop:dt.datetime, freq:float):

        # starttime = time.time()
        # while start >= starttime:
            j = runquotes()
            # print (len)
            # time.sleep(freq - ((time.time() - starttime)))
            # if time.time() stop:
            #     break

    def storeCandles(self, symbol, start, end, resolution, key=None, store=['csv']):
        '''
        Query data for candle data for symbol at given start, end and resolution.
        Store it in csv and/or db
        :params store: arr containing combination of ['csv', 'db']
        '''
        origstart = start
        origend = end
        print()
        print(f'Beginning requests for {symbol}')
        while True:
            j = self.getCandles(symbol, start, end, resolution, key)
            if not j or j['s']== 'no_data':
                return
            if 'csv' in store:
                fn = getCsvDirectory() + f'/{symbol}_{start}_{end}_{resolution}.csv'
                # If the exact fn exists, the data should be the same
                with open(fn, 'w', newline='') as csvfile:
                    csv_writer = csv.writer(csvfile)
                    # ['symbol', 'close', 'high', 'low', 'open', 'price', 'time', 'vol']
                    header =  ['close', 'high', 'low', 'open', 'time', 'vol']
                    i = 0
                    for c, h, l, o, t, v in zip(j['c'], j['h'], j['l'], j['o'], j['t'], j['v']):
                        if i == 0:
                            csv_writer.writerow(header)
                        csv_writer.writerow([c, h, l, o, t, v])
                        i += 1
                    print(f'Wrote {i} records to {fn}')
            if 'db' in store:

                mc = ManageCandles(getSaConn())
                candles = []
                for c, h, l, o, t, v in zip(j['c'], j['h'], j['l'], j['o'], j['t'], j['v']):
                    candles.append([c, h, l, o, t, v])
                CandlesModel.addCandles(symbol, candles, mc.engine)
            dmin, dmax = min(j['t']), max(j['t'])
            if dmin > origstart:
                end = dmin-1
                start = end -  int(dt.timedelta(days=29).total_seconds())
            else:
                break
    

    def getCandles(self, symbol, start, end, resolution, key=None):
        '''
        :symbol: The ticker to get
        :start: Unixtime. The requested start time for finnhub data.
        :end: Unixtime. The requested end time for finnhbub data.
        :interval: The candle interval. Must be one of [1, 5, 15, 30, 60, 'D', 'W', 'M']
        '''
        # base = 'https://finnhub.io/api/v1/stock/candle?'
        retries = 5
        sleeptime = 5
        params = {} 
        params['symbol'] = symbol
        params['from'] = start
        params['to'] = end
        params['resolution'] = resolution

        params['token'] = getFhToken() if key is None else key
        
        response = requests.get(self.CANDLES, params=params)

        meta = {'code': response.status_code}
        while retries > 0:
            if response.status_code != 200:
                logging.error(response.content)
                print("ERROR", response.content)
                return None
            j = response.json()
        
            meta['message'] = j['s']
            if 'o' not in j.keys():
                if retries > 0:
                    print(f'Error-- no data for {symbol}. Retrying after a short sleep', symbol)
                    print(response.url)
                    time.sleep(sleeptime)
                retries -= 1
            else: 
                retries = 0
        return j

    def getTickers(self):
        j = self.runquotes()
        return list(j.keys())
        

def runit():
    start = dt.datetime.now() + dt.deltatime(seconds=90)
    stop = dt.datetime.now() + dt.deltatime(seconds=190)
    freq = dt.deltatime(seconde=15)

    sq = StockQuote()
    sq.getQuotes(start, stop, freq)



def example():
    symbol = 'ATVI'
    start = dt2unix(dt.datetime(2019, 1, 1))
    end = dt2unix(dt.datetime.now())
    resolution = 1

    sq = StockQuote()
    mq = ManageQuotes(getSaConn())

    # This is an example of how to call store Candles. The result is currently to save a file
    sq.storeCandles(symbol, start, end, resolution)
    # QuotesModel.addQuotes(sq.runquotes(), mq.engine)


def example2():
    sq = StockQuote()
    tick = sq.getTickers()
    before = time.perf_counter()
    j = sq.runSingleQuotes(tick)
    print(time.perf_counter() - before)

def nasdaq(start, end, tickers=None):
    sq = StockQuote()
    if tickers == None:
        tickers = nasdaq100symbols[::-1]
    for ticker in tickers:
        sq.storeCandles(ticker, start, end, 1, store=['db'])


def devexamp(symbol, start, end):
    sq = StockQuote()
    start = dt2unix(dt.datetime.now() - dt.timedelta(days=180))
    end = dt2unix(dt.datetime.now())
    sq.storeCandles(symbol, start, end, 1, store=['db'])


    
if __name__ == '__main__':
    # devexamp()
    start = dt2unix(dt.datetime(2020, 1, 1))
    end = dt2unix(dt.datetime.now())
    # tickers = ['TXN', 'SNPS', 'SPLK', 'PTON', 'CMCSA', 'GOOGL']
    # tickers = ['PTON', 'CMCSA', 'GOOGL']
    # tickers = sp500symbols
    mc = ManageCandles(getSaConn())
    fn = getCsvDirectory() + '/report.csv'
    tickers = mc.chooseFromReport(fn, numRecords=0)
    nasdaq(start, end, tickers=tickers)
    # symbol = 'ROST'
    # devexamp(symbol, start, end)
    print('done')
