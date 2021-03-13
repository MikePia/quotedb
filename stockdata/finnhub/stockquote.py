import csv
import logging
import requests
import time
import datetime as dt
import pandas as pd

from models.finntickmodel import FinnTickModel, ManageFinnTick
from models.candlesmodel import CandlesModel, ManageCandles
from models.quotemodel import QuotesModel, ManageQuotes
from qexceptions.qexception import InvalidServerResponseException
from stockdata.sp500 import nasdaq100symbols
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

    manageCandles = None
    __manageQuotes = None

    def __init__(self, tickers, dadate, limit=25000):
        self.tickers = tickers
        self.date = dadate
        self.limit = limit
        self.cycle = {k: 0 for k in tickers}

    def getManageQuotes(self, reinit=False):
        if self.__manageQuotes is None or reinit:
            self.__manageQuotes = ManageQuotes(getSaConn(), create=True)
        return self.__manageQuotes

    def getQuote(self, symbol):
        retries = 5

        if symbol is None:
            symbol = self.tickers[0]

        params = {}
        params['symbol'] = symbol
        while retries > 0:
            try:
                response = requests.get(self.SINGLEQUOTE, params=params, headers=self.HEADERS)
            except Exception as ex:
                print(ex)
                logging.warning(ex)
                response -= 1
                j = None
                continue
            status = response.status_code
            if status != 200:
                logging.error(response.content)
                print("ERROR", response.content)
                j = None
                retries -= 1
            else:
                retries = 0
                j = response.json()
        return j

    # TODO: use Twitsted or Chronus
    def getQuotes(self, store=False):
        quotes = []
        for ticker in self.tickers:
            quote = self.getQuote(ticker)
            if quote:
                quote['s'] = ticker
                quotes.append(quote)
        if store and quotes:
            mq = self.getManageQuotes()
            QuotesModel.addQuotes(quotes, mq.session)
            pass
        return quotes

    def cycleQuotes(self, start: dt.datetime, stop: dt.datetime, freq: float, store=True):
        """
        The quote endpoint only gets current data. If end < now, nothing will be gotten
        """
        now = time.time()
        start = dt2unix(start, unit='s')
        end = dt2unix(stop, unit='s')
        curr = start
        if curr > now:

            print(f'waiting {curr-now} seconds to start')
            time.sleep(curr-now)
        while curr >= start and curr < end:
            self.getQuotes(store=True)
            startnext = curr + freq
            curr = time.time()
            if curr > end:
                break
            if startnext > curr:
                time.sleep(startnext-curr)

    def storeCandles(self, ticker, end, resolution, key=None, store=['csv']):
        '''
        Query data for candle data for ticker at given start, end and resolution.
        Store it in csv and/or db
        :params store: arr containing combination of ['csv', 'db']
        '''
        print()
        print(f'Beginning requests for {ticker}')
        while True:
            j = self.getCandles(ticker, self.cycle[ticker], end, resolution, key)
            if not j or j['s'] == 'no_data':
                return
            if 'csv' in store:
                fn = getCsvDirectory() + f'/{ticker}_{self.cycle[ticker]}_{end}_{resolution}.csv'
                # If the exact fn exists, the data should be the same
                with open(fn, 'w', newline='') as csvfile:
                    csv_writer = csv.writer(csvfile)
                    # ['ticker', 'close', 'high', 'low', 'open', 'price', 'time', 'volume']
                    header = ['close', 'high', 'low', 'open', 'time', 'volume']
                    i = 0
                    for c, h, l, o, t, v in zip(j['c'], j['h'], j['l'], j['o'], j['t'], j['v']):
                        if i == 0:
                            csv_writer.writerow(header)
                        csv_writer.writerow([c, h, l, o, t, v])
                        i += 1
                    print(f'Wrote {i} records to {fn}')
            if 'db' in store:

                mc = self.getManageCandles()
                candles = []
                if not j or not j['t']:
                    self.cycle[ticker] = j['t'][-1]+1
                    break
                for c, h, l, o, t, v in zip(j['c'], j['h'], j['l'], j['o'], j['t'], j['v']):
                    candles.append([c, h, l, o, t, v])
                CandlesModel.addCandles(ticker, candles, mc.engine)
            self.cycle[ticker] = j['t'][-1]+1

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

        while retries > 0:
            try:
                response = requests.get(self.CANDLES, params=params)
            except Exception as ex:
                print(ex)
                retries -= 1
                j = None
                continue

            meta = {'code': response.status_code}
            if response.status_code != 200:
                logging.error(response.content)
                print("ERROR", response.content)
                return None
            j = response.json()

            meta['message'] = j['s']
            if 'o' not in j.keys():
                if retries > 0:
                    retries = 0
                    # print(f'Error-- no data for {symbol}. Retrying after a short sleep', symbol)
                    # print(response.url)
                    # time.sleep(sleeptime)
                # retries -= 1
            else:
                retries = 0
        return j

    def getManageCandles(self, reinit=False):
        if self.manageCandles is None or reinit:
            self.manageCandles = ManageCandles(getSaConn(), create=True)
        return self.manageCandles

    def cycleStockCandles(self, start, latest=False, numcycles=999999999):
        """
        :params lastst: bool, if True, get the max time from the db for  initial start time
        :params start: int, unix time. The time to get data from, overridden if latest is True
        """
        mc = self.getManageCandles()
        start = dt2unix(start, unit='s') if start else None
        end = dt2unix(pd.Timestamp.now(tz="UTC").replace(tzinfo=None), unit='s')
        if latest:
            startTimes = mc.getMaxTimeForEachTicker(self.tickers)
        for t in self.cycle:
            self.cycle[t] = start if not latest else startTimes.get(t, 0)
        while True:
            for ticker in self.tickers:
                self.storeCandles(ticker, end, 1, store=["db"])
            print(f"===================== Cycled through {len(self.tickers)} stocks")
            if numcycles == 0:
                break
            numcycles -= 1
            end = dt2unix(pd.Timestamp.now(tz="UTC").replace(tzinfo=None), unit='s')

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


def nasdaq(start, end, tickers=None):
    sq = StockQuote()
    if tickers is None:
        tickers = nasdaq100symbols[::-1]
    for ticker in tickers:
        sq.storeCandles(ticker, end, 1, store=['db'])


def devexamp(symbol, start, end):
    sq = StockQuote()
    sq.storeCandles(symbol, end, 1, store=['db'])


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
    stocks = nasdaq100symbols
    sq = StockQuote(stocks, None)
    # print(sq.getQuote("ROKU"))
    s = pd.Timestamp("2021-3-8 15:30", tz="US/Eastern").tz_convert("UTC")
    start = dt.datetime(s.year, s.month, s.day, s.hour, s.minute)

    sq.cycleStockCandles(start)


def exampleCycleQuote():
    stocks = ['ADP', 'GOOGL', 'TSLA', 'BKNG', 'TMUS', 'PTON', 'AMZN', 'PEP', 'FAST', 'IDXX']
    # sq = StockQuote(random50(numstocks=10), None)
    sq = StockQuote(stocks, None)

    # start = pd.Timestamp("2011-3-11 12:0:0", tz='US/Eastern').tz_convert('UTC').replace(tzinfo=None)
    start = dt.datetime.utcnow()
    stop = dt.datetime.utcnow() + dt.timedelta(seconds=180)
    freq = 20
    sq.cycleQuotes(start, stop, freq, store=True)


if __name__ == '__main__':
    # dotick()
    # sqstuff()

    exampleCycleQuote()

    print('done')
