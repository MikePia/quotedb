import csv
import logging
import requests
import datetime as dt
import pandas as pd

from models.candlesmodel import CandlesModel, ManageCandles
from stockdata.sp500 import nasdaq100symbols
from stockdata.dbconnection import getFhToken, getSaConn, getCsvDirectory
from utils.util import dt2unix, unix2date


class FinnCandles:

    BASEURL = "https://finnhub.io/api/v1/"
    CANDLES = BASEURL + "stock/candle?"
    HEADERS = {'Content-Type': 'application/json', 'X-Finnhub-Token': getFhToken()}

    bdate = None
    tickers = []

    manageCandles = None
    __manageQuotes = None

    def __init__(self, tickers, limit=25000):
        self.tickers = tickers
        self.limit = limit
        self.cycle = {k: 0 for k in tickers}

    def storeCandles(self, ticker, end, resolution=1, key=None, store=['csv']):
        '''
        Query data for candle data for ticker at given start, end and resolution.
        Store it in csv and/or db
        :params store: arr containing combination of ['csv', 'db']
        '''
        print()
        print(f'Beginning requests for {ticker}')
        j = self.getDateRange(ticker, self.cycle[ticker], end, resolution)
        # while True:
        if not j:
            return
        if 'csv' in store:
            fn = getCsvDirectory() + f'/{ticker}_{self.cycle[ticker]}_{end}_{resolution}.csv'
            # If the exact fn exists, the data should be the same
            with open(fn, 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                # ['ticker', 'close', 'high', 'low', 'open', 'price', 'time', 'volume']
                header = ['close', 'high', 'low', 'open', 'time', 'volume']
                i = 0
                for row in j:
                    if i == 0:
                        csv_writer.writerow(header)
                        i = 99
                    csv_writer.writerow(row)
                print(f'Wrote {i} records to {fn}')
        if 'db' in store:

            mc = self.getManageCandles()
            CandlesModel.addCandles(ticker, j, mc.engine)
        self.cycle[ticker] = j[-1][4]+1

    def getCandles(self, symbol, start, end, resolution=1):
        '''
        Explanation
        -----------
        The atomic function for calling the candle endpoint. Finnhub will retrive the most recent of the
        requested date range. Paginate by changind the end parameter to the first retrieved of a call
        ---------
        :params symbol: list: The ticker to get
        :params start: int: Unixtime. The requested start time for finnhub data.
        :params end: int:  Unixtime. The requested end time for finnhbub data.
        :params resolution int: The candle interval. Must be one of [1, 5, 15, 30, 60, 'D', 'W', 'M']
        '''
        # base = 'https://finnhub.io/api/v1/stock/candle?'
        retries = 5
        # sleeptime = 5
        params = {}
        params['symbol'] = symbol
        params['from'] = start
        params['to'] = end
        params['resolution'] = resolution

        # params['token'] = getFhToken() if key is None else key

        while retries > 0:
            try:
                response = requests.get(self.CANDLES, params=params, headers=self.HEADERS)
            except Exception as ex:
                print(ex)
                retries -= 1
                j = None
                continue

            if response.status_code != 200:
                logging.error(response.content)
                print("ERROR", response.content)
                return None
            j = response.json()

            if 'o' not in j.keys():
                if retries > 0:
                    retries = 0
            else:
                retries = 0
        return j

    def getDateRange(self, symbol, start, end, resolution=1):
        '''
        Explanation
        -----------
        Paginate end to retrieve all the available candles between start and end for one stock.

        '''
        total = []
        while True:
            j = self.getCandles(symbol, start, end, resolution)
            if not j or j['s'] == 'no_data':
                break

            end = j['t'][0] - 1
            print(len(j['t']), unix2date(end))
            j = [[c, h, l, o, t, v] for c, h, l, o, t, v in zip(j['c'], j['h'], j['l'], j['o'], j['t'], j['v'])]

            total.extend(j)
        return total

    def getManageCandles(self, reinit=False):
        if self.manageCandles is None or reinit:
            self.manageCandles = ManageCandles(getSaConn(), create=True)
        return self.manageCandles

    def cycleStockCandles(self, start, latest=False, numcycles=999999999):
        """
        Explanation
        ___________
        Retrieve candles for self.tickers repeatedly. The result will be to bring the database up to date
        then continue to keep it current. Set numcycles to 0 or 1 to just bring it up to date and quit

        Parameters
        _________
        :params start: int: unix time. 
            The time to get data from, overridden if latest is True and the max time is greater than start
        :params latest: bool:
            True, get the max time from the db for  initial start time
        :params numcycles: int
            Use this to truncate the loop.
        """
        mc = self.getManageCandles()
        # start = dt2unix(start, unit='s') if start else 0
        end = dt2unix(pd.Timestamp.now(tz="UTC").replace(tzinfo=None), unit='s')
        if latest:
            startTimes = mc.getMaxTimeForEachTicker(self.tickers)
        for t in self.cycle:
            self.cycle[t] = start if not latest else max(startTimes.get(t, 0), start)
        while True:
            for ticker in self.tickers:
                self.storeCandles(ticker, end, 1, store=["db"])
            print(f"===================== Cycled through {len(self.tickers)} stocks")
            if numcycles == 0:
                break
            numcycles -= 1
            end = dt2unix(pd.Timestamp.now(tz="UTC").replace(tzinfo=None), unit='s')


if __name__ == '__main__':
    ##############################################
    # fc = FinnCandles([])
    # # stocks = nasdaq100symbols
    # ticker = 'AAPL'
    # start = dt2unix(dt.datetime(2021, 1, 1), unit='s')
    # end = pd.Timestamp.utcnow().tz_convert('US/Eastern').replace(tzinfo=None)
    # print(end)
    # end = dt2unix(end, unit='s')
    # fc.getDateRange(ticker, start, end)
    ##################################################
    gc = FinnCandles(nasdaq100symbols)
    start = dt2unix(dt.datetime(2021, 2, 1), unit='s')
    gc.cycleStockCandles(start, latest=True)

    print('done')
