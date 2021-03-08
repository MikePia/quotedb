import csv
import logging
import requests
import datetime as dt
import pandas as pd

from models.finntickmodel import FinnTickModel, ManageFinnTick
from models.candlesmodel import CandlesModel, ManageCandles
from models.quotemodel import QuotesModel
from qexceptions.qexception import InvalidServerResponseException
from stockdata.sp500 import nasdaq100symbols, random50
from stockdata.dbconnection import getFhToken, getSaConn, getCsvDirectory
from utils.util import dt2unix


class StockQuote:

    BASEURL = "https://finnhub.io/api/v1/"
    QUOTES = BASEURL + "quote/us?"
    CANDLES = BASEURL + "stock/candle?"
    SINGLEQUOTE = BASEURL + "quote?"
    TICKS = BASEURL + "stock/tick?"
    HEADERS = {'Content-Type': 'application/json', 'X-Finnhub-Token': getFhToken()}

    bdate = None
    tickers = []

    def __init__(self, tickers, dadate, limit=25000):
        self.tickers = tickers
        self.date = dadate
        self.limit = limit
        self.cycle = {k: 0 for k in tickers}

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
        pass
        # base = self.QUOTES
        # params = {}
        # params['token'] = 'c0b4p7748v6sc0gs4oq0'
        # response = requests.get(self.QUOTES, headers=self.HEADERS)
        # response = requests.get(self.QUOTES, params=params)
        # status = response.status_code
        # if status != 200:
        #     raise
        # return response.json()

    # TODO: use Twitsted or Chronus
    def getQuotes(self, start: dt.datetime, stop: dt.datetime, freq: float):
        pass
        # starttime = time.time()
        # while start >= starttime:
        #     j = runquotes()
        #     print (len)
        #     time.sleep(freq - ((time.time() - starttime)))
        #     if time.time() stop:
        #         break

    def storeCandles(self, symbol, start, end, resolution, key=None, store=['csv']):
        '''
        Query data for candle data for symbol at given start, end and resolution.
        Store it in csv and/or db
        :params store: arr containing combination of ['csv', 'db']
        '''
        origstart = start
        print()
        print(f'Beginning requests for {symbol}')
        while True:
            j = self.getCandles(symbol, start, end, resolution, key)
            if not j or j['s'] == 'no_data':
                return
            if 'csv' in store:
                fn = getCsvDirectory() + f'/{symbol}_{start}_{end}_{resolution}.csv'
                # If the exact fn exists, the data should be the same
                with open(fn, 'w', newline='') as csvfile:
                    csv_writer = csv.writer(csvfile)
                    # ['symbol', 'close', 'high', 'low', 'open', 'price', 'time', 'vol']
                    header = ['close', 'high', 'low', 'open', 'time', 'vol']
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
            dmin = min(j['t'])
            if dmin > origstart:
                end = dmin-1
                start = end - int(dt.timedelta(days=29).total_seconds())
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
        # sleeptime = 5
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
                    # time.sleep(sleeptime)
                retries -= 1
            else:
                retries = 0
        return j

    def getTickers(self):
        j = self.runquotes()
        return list(j.keys())

    def __getTicks(self, symbol, skip=0):
        """
        :params bdeate: dt.date
        :params skip: int offset by number of records to paginate
        """
        params = {}
        params['symbol'] = symbol
        params['date'] = self.date.strftime("%Y-%m-%d")
        params['limit'] = self.limit
        params['skip'] = skip
        params['format'] = 'json'
        url = self.TICKS
        retries = 3
        for i in range(retries):
            r = requests.get(url, params=params, headers=self.HEADERS)
            if r.status_code != 200:
                msg = f"Server error while trying: {r.url} at {dt.datetime.now()}"
                if i < retries-1:
                    msg += ": Retrying"
                    logging.error(msg)
                else:
                    msg += "raising exception"
                    logging.error(msg)
                    raise InvalidServerResponseException(msg)
            else:
                break
        j = r.json()
        return j

    def __getTicksOnDay(self,  ticker, skip=0, startat=None):
        '''
        Paginate through the entire day or locate and start with the first tick at startat
        :params startat: pd.Timedelta: Searches and finds the ticks that startat time from
        the end of the data. For ex pd.Timedelta(minutes=30) Will begin data retrieval of the
        last 30 minutes of ticks
        '''
        total = []
        # totalhits = 0  # gets set after we see the first data
        notdone = True
        if startat is not None:
            total = self.__getDataFromHere(ticker, startat)
            notdone = False
            
        while notdone:
            j = self.__getTicks(ticker, self.cycle[ticker])

            if not j or not j['t']:
                break
            # tot = [price, time, volume, condition  for price, time, volume, condition in zip(j['p'], j['t'], j['v'], j['c'])]
            total.extend([x for x in zip(j['p'], j['t'], j['v'], j['c'])])
            self.cycle[ticker] += j['count']
            if j['count'] < self.limit:
                print('done here - go save the world')
                break
        if not total:
            return None
        mft = ManageFinnTick(getSaConn())
        FinnTickModel.addTicks(ticker, total, mft.session)

    def cycleStockTicks(self, startat=None):
        while True:
            for ticker in self.tickers:
                self.__getTicksOnDay(ticker, startat=startat)
            startat = None
            print(f'==================== completed a cycle of {len(self.tickers)} stocks')

    def __getDataFromHere(self, ticker, startat):
        """
        Get the last x minutes of tick data for self.date.
        :params ticker: str, A single stock symbol
        :params j: dict, The reusults of tick endpoint call with skip=0
        :params startat: pd.Timedelta, Instruction to get the last {startat} of time from the requested day
        """
        # Have to do the first one just to find out how many records are available
        total = []
        j = self.__getTicks(ticker, self.cycle[ticker])
        if not j or not j['t']:
            return []
        self.cycle[ticker] = j['total'] - self.limit
        j = self.__getTicks(ticker, self.cycle[ticker])
        curmaxtime = j['t'][-1]
        begin_time = int(curmaxtime - (startat.total_seconds() * 1e3))
        curmintime = j['t'][0]
        done = False
        total.extend([x for x in zip(j['p'], j['t'], j['v'], j['c'])])

        if curmintime <= begin_time <= curmaxtime:
            self.cycle[ticker] = j['total']
            done = True
        while not done:
            self.cycle[ticker] -= len(j['t'])
            j = self.__getTicks(ticker, self.cycle[ticker])
            # These are strangely out of order now beginning wi the send pull
            total.extend([x for x in zip(j['p'], j['t'], j['v'], j['c'])])
            if begin_time > j['t'][0]:
                assert j['t'][0] <= begin_time <= j['t'][-1]
                self.cycle[ticker] = j['total']
                break
            if self.limit > len(j['t']):
                logging.warn(f"Not all data is available for the day: {self.date}")
                self.cycle[ticker] = j['total']
                break
        # total = sorted(total, key=lambda tick: tick[1])
        # Stopat should be in the from limit amount. so do a binary search betwwen 0 and limit
        # pandas would be a lot cleaner
        total = sorted(total, key=lambda tick: tick[1])
        found = False
        high = min(self.limit-1, len(total)-1)
        low, cur = 0,  high//2
        while not found:

            # Binary search for first and lowest value >= begin_time
            # (Note that the search has not accounted for when cur[0] == begin_time), need to get the
            #   previous (time-wise) one down for possible repeated ticks at next total[limit])
            # First case found it precisely, find the first occurrence (of non duplicate) cur = i
            assert total[0][1] <= begin_time <= total[-1][1]
            assert total[low][1] <= begin_time <= total[high][1]

            if total[cur][1] == begin_time:
                i = cur - 1
                while i > low and total[i][1] == begin_time:
                    i -= 1
                cur = i
                break
            # Second case three possibilities
            #   1-2) cur < begin_time and (cur+1 > begin_time or cur+1 == begin_time): cur = cur + 1 and done
            #   3)                and cur+1 < begin_time: low = cur, cur = (low+high)//2
            elif total[cur][1] < begin_time:

                if total[cur+1][1] >= begin_time:
                    cur += 1
                    break
                else:
                    low = cur
                    cur = (low + high) // 2

            # Third cas
            # 1) cur > begin_time and (cur-1 < begin_time) (cur = cur) done
            # 2)              and (cur-1 = begin_time), find first occurrence of cur-1 and done
            # 3)              and (cur-1 > begin_time), high = cur-1, cur = (low+high) //2
            elif total[cur][1] > begin_time:
                if total[cur-1][1] < begin_time:
                    break
                elif total[cur-1][1] == begin_time:
                    i = cur - 1
                    while total[i][1] == begin_time:
                        i -= 1
                    cur = i
                    break
                elif total[cur-1][1] > begin_time:
                    high = cur-1
                    cur = (low+high)//2
        total = total[cur:]

        return total


def runit():
    start = dt.datetime.now() + dt.deltatime(seconds=90)
    stop = dt.datetime.now() + dt.deltatime(seconds=190)
    freq = dt.deltatime(seconde=15)

    sq = StockQuote()
    sq.getQuotes(start, stop, freq)


def example():
    symbol = 'ROKU'
    start = dt2unix(dt.datetime(2021, 1, 1))
    end = dt2unix(dt.datetime.now())
    resolution = 1

    sq = StockQuote()
    # mq = ManageQuotes(getSaConn())

    # This is an example of how to call store Candles. The result is currently to save a file
    sq.storeCandles(symbol, start, end, resolution)
    QuotesModel.addQuotes(sq.runquotes(), mq.engine)


# def example2():
#     sq = StockQuote()
#     tick = sq.getTickers()
#     j = sq.runSingleQuotes(tick)

def nasdaq(start, end, tickers=None):
    sq = StockQuote()
    if tickers is None:
        tickers = nasdaq100symbols[::-1]
    for ticker in tickers:
        sq.storeCandles(ticker, start, end, 1, store=['db'])


def devexamp(symbol, start, end):
    sq = StockQuote()
    sq.storeCandles(symbol, start, end, 1, store=['db'])


def dotick():
    begtime = dt.datetime(2021, 3, 5)
    sq = StockQuote(sorted(nasdaq100symbols)[50:], begtime, limit=25000)
    # sq = StockQuote(random50(numstocks=5), begtime)
    # sq = StockQuote(['AAPL', 'TSLA'], begtime, limit=25000)

    # print(sq._StockQuote__getTicks("SQ", begtime))
    delt = pd.Timedelta(hours=2)
    # sq._StockQuote__getTicksOnDay('SQ', startat=delt)
    sq.cycleStockTicks(startat=delt)


def sqstuff():
    sq = StockQuote(None, None)
    print(sq.getSingleQuote("ROKU"))


if __name__ == '__main__':
    # dotick()
    sqstuff()

    # start = dt2unix(dt.datetime(2021, 2, 1))
    # end = dt2unix(dt.datetime.now())
    # devexamp("PDD", start, end)

    # mc = ManageCandles(getSaConn())
    # fn = getCsvDirectory() + '/report.csv'
    # tickers = mc.chooseFromReport(fn, numRecords=0)
    # nasdaq(start, end, tickers=tickers)
    # symbol = 'ROST'
    # devexamp(symbol, start, end)
    print('done')
