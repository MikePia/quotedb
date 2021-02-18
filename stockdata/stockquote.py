import csv
import json
import logging
import requests
import time
import datetime as dt

from models.quotemodel import QuotesModel, ManageQuotes
from qexceptions.qexception import InvalidServerResponseException
from stockdata.dbconnection import getFhToken, getSaConn, getCsvDirectory
from utils.util import dt2unix


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
        Many ways to handle this. Just going to implement one till for now
        :params store: arr containing combination of ['csv', 'db']
        Note that the 
        '''
        j = self.getCandles(symbol, start, end, resolution, key)
        if not j:
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
                        i = 1234
                    csv_writer.writerow([c, h, l, o, t, v])
        if 'db' in store:
            print('Database storage is not implemented')
        print()
        

    def getCandles(self, symbol, start, end, resolution, key=None):
        '''
        :symbol: The ticker to get
        :start: Unixtime. The requested start time for finnhub data.
        :end: Unixtime. The requested end time for finnhbub data.
        :interval: The candle interval. Must be one of [1, 5, 15, 30, 60, 'D', 'W', 'M']
        '''
        # base = 'https://finnhub.io/api/v1/stock/candle?'
        params = {} 
        params['symbol'] = symbol
        params['from'] = start
        params['to'] = end
        params['resolution'] = resolution

        params['token'] = getFhToken() if key is None else key
        
        response = requests.get(self.CANDLES, params=params)

        meta = {'code': response.status_code}
        if response.status_code != 200:
            logging.error(response.content)
            if response.status_code == 429:
                # TODO
                d = dt.datetime.now()
            return None
        j = response.json()
        meta['message'] = j['s']
        if 'o' not in j.keys():
            logging.info('Error-- no data')
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
    symbol = 'NIO'
    start = dt2unix(dt.datetime.now()-dt.timedelta(days=10))
    end = dt2unix(dt.datetime.now()-dt.timedelta(days=5))
    resolution = 5

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



    
        
if __name__ == '__main__':
    print(getSaConn())
    example2()
    # mq = ManageQuotes('sqlite:///quotes.sqlite', True)
    # mq = ManageQuotes('sqlite:///quotes.sqlite', True)
    # lh = "mysql+pymysql://stockdbuser:Kwk78?l8@localhost/stockdb"
    # mq = ManageQuotes(lh, True)


    # j = sq.getCandles()
    # QuotesModel.addQuotes(j, mq.engine)
    print('done')
