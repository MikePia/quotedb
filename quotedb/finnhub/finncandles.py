import csv
import json
import logging
import requests
# import datetime as dt
import threading
import pandas as pd

from quotedb.models.candlesmodel import CandlesModel
from quotedb.models.managecandles import ManageCandles
from quotedb.models.managetopquotes import ManageTopQuote
from quotedb.dbconnection import getFhToken, getSaConn, getCsvDirectory
from quotedb.utils.util import dt2unix  # , unix2date


class FinnCandles:

    BASEURL = "https://finnhub.io/api/v1/"
    CANDLES = BASEURL + "stock/candle?"
    SYMBOLS = BASEURL + "stock/symbol?exchange=US"

    HEADERS = {'Content-Type': 'application/json', 'X-Finnhub-Token': getFhToken()}

    bdate = None
    tickers = []

    manageCandles = None
    __manageQuotes = None

    def __init__(self, tickers, limit=25000):
        self.tickers = tickers
        self.limit = limit
        self.cycle = {k: 0 for k in tickers}

    def wrapStoreCandles(self, args):
        '''
        The multiprocessor mod requires a single argument for a target.
        '''
        return self.storeCandles(*args)

    def storeCandles(self, ticker, end, start=None,  model=CandlesModel, key=None, store=['csv'], fq_time=None, manager=None, resolution=1):
        '''
        Explanataion
        ------------
        Get data from finnhub for candle data for ticker at given start, end and resolution.
        Save to file and/or db. Will write to at most one file. Preference in order is
        json, csv. This method processes data from one stock. The file name will
        be {stock}_{start}_{end}_{resolution}.{format}. The file will be written to the installation
        defined directory accessed by getCsvDirectory()

        Paramaters
        ----------
        :params ticker: str: the stock ticker
        :params end: int: Unix time
        :params start: int: Unix time. If called directly, use this paramater. Internal calls, e.g.
            from cycleStocks, populate self.cycle.stocks with current max date
        :params resolution: int: The candle interval. Generally leave the default of 1
        :params model: db model: [CandlesModel, AllquotesModel, TopquotesModel]
        :params store: arr containing combination of ['json', csv', db']
        :params return: str: the filename. Will be None if no file was requested
        TODO: implement json
        '''
        # print()
        print(f'Beginning requests for {ticker}: ', end='')
        if start:
            self.cycle[ticker] = max(start, self.cycle[ticker])
        j = self.getDateRange(ticker, self.cycle[ticker], end, resolution)
        # while True:
        if not j:
            return
        fn = None
        if 'json' in store:
            print('json not implemented in FinnCandles.storeCandles')
            fn = getCsvDirectory() + f'/{ticker}_{self.cycle[ticker]}_{end}_{resolution}.json'
            jd = {ticker: [{'close': x[0], 'high': x[1], 'low': x[2], 'open': x[3], 'timestamp': x[4], 'volume': x[5]} for x in j]}
            with open(fn, 'w', newline='') as f:
                f.write(json.dumps(jd))
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
            if model.__tablename__ != "topquotes":
                fq_time = None

            if manager:
                mc = manager
            else:
                mc = self.getManageCandles(model, reinit=True, fq_time=-1)
            retries = 5
            while retries > 0:
                try:
                    mc.addCandles(ticker, j, mc.session)
                    retries = 0

                except Exception as ex:
                    retries -= 1
                    logging.error("Db failed, retrying")
                    logging.error(ex)

        self.cycle[ticker] = max(j[-1][4]+1, self.cycle[ticker])
        return fn

    def getCandles_fh(self, symbol, start, end, resolution=1):
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
        j = {}
        while retries > 0:
            try:
                response = requests.get(self.CANDLES, params=params, headers=self.HEADERS)
            except Exception as ex:
                print(ex)
                retries -= 1
                j = None
                continue

            if response.status_code != 200:
                retries -= 1
                logging.error(f"ERROR while processing {symbol}, start {start}: {response.status_code}: {response.reason}: {retries}")
                logging.error(response.url)
                continue

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

        Paramaters
        ----------
        :params symbol: str
        :params start: int: Unix time
        :params end: int: Unix time
        :params return: list<list>: [close, high, low, open, timestamp, volume]

        '''
        total = []
        while True:
            j = self.getCandles_fh(symbol, start, end, resolution)
            if not j or j['s'] == 'no_data':
                break

            end = j['t'][0] - 1
            j = [[c, h, l, o, t, v] for c, h, l, o, t, v in zip(j['c'], j['h'], j['l'], j['o'], j['t'], j['v'])]

            total.extend(j)
            if abs(start-end) < 60:
                break
        return total

    def getManageCandles(self, model, reinit=False, fq_time=None):
        """
        When retrieveing a TopquotesModel, be sure either data already exists in topquotes or
        you have provided a fq_time to create one.
        """
        if self.manageCandles is None or reinit:
            if model.__tablename__ == "topquotes":
                self.manageCandles = ManageTopQuote(self.tickers, model, fq_time=fq_time, create=True)
            else:
                self.manageCandles = ManageCandles(getSaConn(), model, create=True)
        return self.manageCandles

    def cycleStockCandles(self, start, model=CandlesModel, latest=False, numcycles=999999999, fq_time=None):
        """
        Explanation
        -----------
        Retrieve candles for self.tickers repeatedly. The result will be to bring the database up to date
        then continue to keep it current. Set numcycles to 0 or 1 to just bring it up to date and quit.
        Without a low numcycles, runs continuously till stopped externally.

        Parameters
        ----------
        :start: int: unix time.
        ------
            The time to get data from, overridden if latest is True and the max time is greater than start
        :latest: bool:
        -------
            True, get the max time from the db for  initial start time
        :numcycles: int
        -----------
            The number of times to repeat the complete cycle
        :fq_time: int or None: Unix time
        --------
            This argument will be used to install a Firstquote if the model is Topquotes
        """
        mc = self.getManageCandles(model, reinit=True, fq_time=fq_time)
        # start = dt2unix(start, unit='s') if start else 0
        end = dt2unix(pd.Timestamp.now(tz="UTC").replace(tzinfo=None), unit='s')
        if latest:
            startTimes = mc.getMaxTimeForEachTicker(self.tickers)
        for t in self.cycle:
            self.cycle[t] = start if not latest else max(startTimes.get(t, 0), start)
        print(f'Going to retrieve data from finnhub for {len(self.tickers)} stocks, and place them in {model.__tablename__}')
        while True:
            for i, ticker in enumerate(self.tickers):
                print(f'\n{i+1}/{len(self.tickers)}: ', end='')
                self.storeCandles(ticker, end, model=model, store=["db"], manager=mc)
            print(f"===================== Cycled through {len(self.tickers)} stocks")
            if numcycles == 0:
                break
            numcycles -= 1
            end = dt2unix(pd.Timestamp.now(tz="UTC").replace(tzinfo=None), unit='s')

    def cycleStockCandles_mp(self, start, model=CandlesModel, latest=False, numcycles=999999999):
        """
        Explanation
        ___________
        The Multiprocess version
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
        mc = self.getManageCandles(model)
        # start = dt2unix(start, unit='s') if start else 0
        end = dt2unix(pd.Timestamp.now(tz="UTC").replace(tzinfo=None), unit='s')
        if latest:
            startTimes = mc.getMaxTimeForEachTicker(self.tickers)
        for t in self.cycle:
            self.cycle[t] = start if not latest else max(startTimes.get(t, 0), start)
        # groupby = 15
        while numcycles > 0:
            threads = []
            for i in range(0, len(self.tickers)-1):
                # args = (self.tickers[i], end, i, store = ['db'])
                try:
                    args = (self.tickers[i], end, 1, None, model,  ['db'])
                    t = threading.Thread(target=self.storeCandles, args=args)
                    t.start()
                    threads.append(t)
                    for thread in threads:
                        thread.join()
                except Exception as ex:
                    print(ex)

            #     # ticker, end, resolution=1, key=None, store=['csv'
            # for i, ticker in enumerate(self.tickers):
            #     print(f'\n{i}/{len(self.tickers)}: ', end='')
            #     self.storeCandles(ticker, end, 1, store=["db"])
            print(f"===================== Cycled through {len(self.tickers)} stocks")
            if numcycles == 0:
                break
            numcycles -= 1
            end = dt2unix(pd.Timestamp.now(tz="UTC").replace(tzinfo=None), unit='s')

    def getSymbols(self):
        retries = 5
        j = {}
        while retries > 0:
            try:
                response = requests.get(self.SYMBOLS, headers=self.HEADERS)
            except Exception as ex:
                print(ex)
                retries -= 1
                j = None
                continue

            if response.status_code != 200:
                retries -= 1
                logging.error(f"ERROR while processing symbols request: {response.status_code}: {response.reason}: {retries}")
                logging.error(response.url)
                continue

            j = response.json()
            df = pd.DataFrame(data=j, columns=list(j[0].keys()))
            df = df[df.mic.isin(['XNYS', 'BATS', 'ARCX', 'XNMS', 'XNCM', 'XNGS', 'IEXG', 'XASE'])]
            symbols = list(df['symbol'])
            retries = 0
        return symbols


# if __name__ == '__main__':
    ##############################################
    # fc = FinnCandles([])
    # # stocks = nasdaq100symbols
    # ticker = 'AAPL'
    # start = dt2unix(dt.datetime(2021, 1, 1), unit='s')
    # end = pd.Timestamp.utcnow().tz_convert('US/Eastern').replace(tzinfo=None)
    # print(end)
    # end = dt2unix(end, unit='s')
    # fc.getDateRange(ticker, start, end)
    # #################################################
    # gc = FinnCandles(nasdaq100symbols)
    # start = dt2unix(dt.datetime(2021, 2, 1), unit='s')
    # gc.cycleStockCandles(start, latest=True)
    # #####################################################
