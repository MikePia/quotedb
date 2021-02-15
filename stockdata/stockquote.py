import csv
import json
import logging
import requests
import time
import datetime as dt

from models.quotemodel import QuotesModel, ManageQuotes
from stockdata.dbconnection import getFhToken, getSaConn
from utils.util import dt2unix

class InvalidServerResponseException(Exception):
    pass

class StockQuote:

    BASEURL = "https://finnhub.io/api/v1/"
    BASEQUOTE = BASEURL + "quote/us?"
    BASECANDLE = BASEURL+ "stock/candle?"
    HEADERS = {'Content-Type': 'application/json', 'Authorization' : 'Token '+ getFhToken()}

    def runquote(self):
        base = self.BASEQUOTE
        params = {}
        params['token'] = 'c0b4p7748v6sc0gs4oq0'
        # response = requests.get(self.BASEQUOTE, headers=self.HEADERS)
        response = requests.get(self.BASEQUOTE, params=params)
        status = response.status_code
        # if status != 200:
        #     raise 
        return response.json()


    # TODO: use Twitsted or Chronus
    def getQuotes(self, start:dt.datetime, stop:dt.datetime, freq:float):
        starttime = time.time()
        while start >= starttime:
            j = runquote()
            # print (len)
            # time.sleep(freq - ((time.time() - starttime)))
            # if time.time() stop:
            #     break


    def storeCondles(self, symbol, start, end, resolution, key=None):
        pass
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

        # params['token'] = getFhToken() if key is None else key
        
        response = requests.get(self.BASECANDLE, params=params)

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


def runit():
    start = dt.datetime.now() + dt.deltatime(seconds=90)
    stop = dt.datetime.now() + dt.deltatime(seconds=190)
    freq = dt.deltatime(seconde=15)

    sq = StockQuote()
    sq.getQuotes(start, stop, freq)


    
        
if __name__ == '__main__':
    print(getSaConn())
    # mq = ManageQuotes(getSaConn())
    # mq = ManageQuotes('sqlite:///quotes.sqlite', True)
    # mq = ManageQuotes('sqlite:///quotes.sqlite', True)
    # lh = "mysql+pymysql://stockdbuser:Kwk78?l8@localhost/stockdb"
    # mq = ManageQuotes(lh, True)
    sq = StockQuote()

    symbol = 'ROKU'
    start = dt2unix(dt.datetime.now()-dt.timedelta(days=3))
    end = dt2unix(dt.datetime.now())
    resolution = 15
    x = sq.getCandles(symbol, start, end, resolution)

    j = sq.getCandles()
    QuotesModel.addQuotes(j, mq.engine)
    print('done')
