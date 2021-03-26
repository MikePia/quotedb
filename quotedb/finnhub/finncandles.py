import csv
import logging
import requests
# import datetime as dt
import threading
import pandas as pd

from quotedb.models.candlesmodel import CandlesModel
from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.firstquotemodel import Firstquote, Firstquote_trades
from quotedb.models.managecandles import ManageCandles
# from quotedb.sp500 import nasdaq100symbols
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

    def storeCandles(self, ticker, end, resolution=1, model=CandlesModel,  key=None, store=['csv']):
        '''
        Query data for candle data for ticker at given start, end and resolution.
        Store it in csv and/or db
        :params store: arr containing combination of ['csv', 'db']
        '''
        # print()
        print(f'Beginning requests for {ticker}: ', end='')
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

            mc = self.getManageCandles(model)
            retries = 5
            while retries > 0:
                try:
                    mc.addCandles(ticker, j, mc.session)
                    retries = 0

                except Exception as ex:
                    retries -= 1
                    logging.error("Db failed, retrying")
                    logging.error(ex)

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

        '''
        total = []
        while True:
            j = self.getCandles(symbol, start, end, resolution)
            if not j or j['s'] == 'no_data':
                break

            end = j['t'][0] - 1
            j = [[c, h, l, o, t, v] for c, h, l, o, t, v in zip(j['c'], j['h'], j['l'], j['o'], j['t'], j['v'])]

            total.extend(j)
            if abs(start-end) < 60:
                break
        return total

    def getManageCandles(self, model, reinit=False):
        if self.manageCandles is None or reinit:
            self.manageCandles = ManageCandles(getSaConn(), model, create=True)
        return self.manageCandles

    def cycleStockCandles(self, start, model=CandlesModel, latest=False, numcycles=999999999):
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
        mc = self.getManageCandles(model)
        # start = dt2unix(start, unit='s') if start else 0
        end = dt2unix(pd.Timestamp.now(tz="UTC").replace(tzinfo=None), unit='s')
        if latest:
            startTimes = mc.getMaxTimeForEachTicker(self.tickers)
        for t in self.cycle:
            self.cycle[t] = start if not latest else max(startTimes.get(t, 0), start)
        tabname = "candles" if model == CandlesModel else "allquotes"
        print(f'Going to retrieve data from finnhub for {len(self.tickers)} stocks, and place them in {tabname}')
        while True:
            for i, ticker in enumerate(self.tickers):
                print(f'\n{i}/{len(self.tickers)}: ', end='')
                self.storeCandles(ticker, end, 1, model=model, store=["db"])
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

    def createFirstQuote(self, timestamp, stocks="all", model=AllquotesModel):
        """
        Explanation
        -----------
        Create a new firstquote or update current. Guarantee that there will be an entry for
        every symbol in stocks as much as possible. Some of listed stocks get an illegal access
        error from finnhub. The user will be best served if the the data has already been collected
        before makeing this call. Call startCandles with a date that precedes timestamp by some amout
        of time.

        Parameters
        ----------
        :params timestamp: int: unixtime
        :params stocks: union[str, list]: "all" will get candles from evey available US exchange. Otherwise
            send a list of the stockss to be included
        :params model: SqlAlchemy model: Currently either CandlesModel or AllqutoesModel. Will determine which table to use.
        """
        # plus = 60*60*3    # The number of seconds to pad the start time.
        stocks = stocks if isinstance(stocks, list) else self.getSymbols() if stocks == "all" else None
        if not stocks:
            logging.info("Invalid request in createFirstQuote")
            return None
        mc = ManageCandles(getSaConn(), model)
        candles = mc.getFirstQuoteData(timestamp)

        fq = [Firstquote_trades(stock=d['stock'],
                                high=d['high'],
                                low=d['low'],
                                open=d['open'],
                                close=d['close'],
                                volume=d['volume']) for d in [dict(x) for x in candles]]
        Firstquote.addFirstquote(timestamp, fq, mc.session)


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
    # gc = FinnCandles(nasdaq100symbols)
    # start = dt2unix(dt.datetime(2021, 2, 1), unit='s')
    # gc.cycleStockCandles(start, latest=True)
    ######################################################
    # fc = FinnCandles([])
    # x = fc.getSymbols()
    # stocks = ['AAPL', 'TSLA', 'ROPKU', 'SQ', 'BBB']
    # fc = FinnCandles(stocks)
    # start = dt2unix(pd.Timestamp(2020, 12, 1))
    # fc.cycleStockCandles_mp(start, CandlesModel)
    # print('done')
    ########################################################
    import datetime as dt
    from quotedb.utils.util import dt2unix_ny

    d = timestamp = dt.datetime(2021,3, 25, 3, 0, 0)
    timestamp = dt2unix_ny(d)
    print('naivie time', d, 'Corresponding utc for newyofk time', timestamp)
    # timestamp = dt2unix(dt.datetime.utcnow()) - (60 * 60 * 5)
    fc = FinnCandles([])
    fc.createFirstQuote(timestamp)