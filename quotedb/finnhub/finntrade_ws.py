import copy
import csv
import datetime as dt
import json
import logging
import os
import pandas as pd
import re
import threading
import time
import websocket
from quotedb.dbconnection import getFhToken, getSaConn, getCsvDirectory
from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.common import createFirstQuote
from quotedb.utils.util import formatFn

from quotedb.models.trademodel import ManageTrade, TradeModel
from quotedb.utils import util


class MyWebSocket(threading.Thread):
    """
    Explanation
    -----------
    Connect to the finnhub websocket trade endpoint. Everything is activated when MyWebSocket
    is created. All the other calls are callbacks and are called by the websocket

    Parameters
    _________
    :params tickers: list: Stocks to subscribe to.
    :params fn: str: Filename to save data if either csv or json are in store
    :params store: list:  May include [csv, json, db]
        Either csv or json can be written to but not both. csv is default. If db is included,
        data will be written to the database.
    :params resample_td: timedelta: If it is not None, aggreagate the results by the amount.
    :params ffill: bool. Is only active if resample_td has a value. If True, provide value for every
        timestamp and stock in self.tickers.
    :params fq: Firstquote or None. If it exists, it indicates data should include delta_p and delat_t.
        The data will be calculated by the difference, for any [timestamp, stock], between the data in
        firstquote and current trade record.

    Programming notes TODO
    ----------------------
    The firstquote timestamp is in seconds. The data from the websocket is milliseconds.
    retool csv, json, and visualize to use the resampled result if it is there. I think
    we need disable db storage for filled data, or create a temporary table on request.
    """
    doreport = False

    def __init__(self, tickers, fn, store=['csv'], resample_td=None, ffill=False, fq=None):
        threading.Thread.__init__(MyWebSocket)
        self.tickers = tickers
        self.delt = resample_td
        # Active only if resample_dt has a value
        self.ffill = ffill
        self.fn = fn
        self.store = store
        self.daemon = True
        self.aggregate = pd.DataFrame()

        # These three are active if fq has a value. The currentquote (cq) will be used to track
        # prices when no trades are happening for a stock. The missing stocks will be replaced with
        # the first data from a trade for a stock.
        self.missing = []
        # self.fq = None
        # self.cq = None
        # if fq:
        #     # We need to be able to select these by stock. Dataframe or dict? go dict
        #     # Have to also change the type of the enclosing fq, it has a relationship with firstquote_trades
        #     # Also changing to 'trade' format (matches ws data)  no high, low or open and price = close
        #     # result is {'timestamp': <int>, 'firstquote_trades': <DataFrame>}
        #     firstquote_trades = {x.stock: [x.close, x.volume] for x in fq.firstquote_trades}
        #     # firstquote_trades = pd.DataFrame([(x.stock, x.close, x.volume) for x in fq.firstquote_trades], columns=cols)
        #     if set(self.tickers) != set(firstquote_trades.keys()):
        #         x = set(self.tickers) - set(firstquote_trades.keys())
        #         if x:
        #             self.missing = list(x)
        #             logging.warning(f'WARNING: Missing information for requested stock(s): {x}')
        #             logging.warning(f'WARNING: Using a firstquote of 0.0 for: {x}. The first returned data will replace the 0.0')
        #         for missing in x:
        #             firstquote_trades[missing] = [0.0, 0]
        #     self.fq = {'timestamp': fq.timestamp, 'firstquote_trades': firstquote_trades}

        #     self.cq = copy.deepcopy(self.fq)
        self.proc = ProcessData(self.tickers, fq, self.delt)
        if self.doreport:
            self.proc.initializeReport(self.__dict__)
        self.keepgoing = True

    def run(self):
        websocket.enableTrace(True)
        if 'db' in self.store:
            self.mt = ManageTrade(getSaConn())
        url = f"wss://ws.finnhub.io?token={getFhToken()}"
        self.ws = websocket.WebSocketApp(url,
                                         on_open=self.on_open,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

        self.ws.run_forever()

    def on_message(self, message):
        # print(message)
        j = json.loads(message)
        if j['type'] == 'trade':
            df = pd.DataFrame(j['data'])
            df = df.rename(columns={'p': 'price', 's': 'stock', 't': 'timestamp', 'v': 'volume'})
            if self.delt is not None:
                # Gathere togethere at least 5 seconds of data to aggregate
                self.aggregate = self.aggregate.append(df)
                times = list(self.aggregate.timestamp)
                if (max(times) - min(times)) >= 15000:
                    if self.ffill:
                        df = self.proc.resampleit_fill(self.aggregate)
                    else:
                        df = self.resampleit()
                    self.aggregate = pd.DataFrame()
                    print('New data to deal with here ...', len(df))
                else:
                    return
            if 'json' in self.store or 'visualize' in self.store or 'csv' in self.store:
                print('.', end='')

                self.proc.writeFile(self.proc.formatData(df, self.store, self.ffill), self.fn, self.store)
                if self.doreport and not self.proc.addToReport(df):
                    self.keepgoing = False
                    self.ws.close()
            if 'db' in self.store:
                TradeModel.addTrades(df, self.mt.engine)
            # if self.store == []:
            #     print()

        else:
            print(message)

    def on_error(self, error):
        print(error)

    def on_close(self):
        print("### closed ###")

    def on_open(self):
        for ticker in self.tickers:
            msg = f'{{"type":"subscribe","symbol":"{ticker}"}}'
            self.ws.send(msg)

    def unsubscribe(self, removit):
        for ticker in removit:
            msg = f'{{"type":"unsubscribe","symbol":"{ticker}"}}'
            self.ws.send(msg)

    def subscribe(self, addit):
        for ticker in addit:
            msg = f'{{"type":"subscribe","symbol":"{ticker}"}}'
            self.ws.send(msg)

    def changesubscription(self, newstocks, newfn=None):
        if newfn:
            self.fn = newfn
        if newstocks:
            unsubscribe = set(self.tickers) - set(newstocks)
            subscribe = set(newstocks) - set(self.tickers)
        else:
            return
        if unsubscribe:
            self.unsubscribe(unsubscribe)
        if subscribe:
            self.subscribe(subscribe)
        self.tickers = newstocks

    def resampleit(self):
        df = self.aggregate

        newdf = pd.DataFrame()
        for count, tm in enumerate(df.timestamp.unique()):
            # First have to consolodate trades with identical timestamps
            x = df[df.timestamp == tm]
            if len(x) > 1:
                for stock in x.stock.unique():
                    x2 = x[x.stock == stock]
                    row = {}
                    row['price'] = sum([x2.iloc[i].price * x2.iloc[i].volume for i in range(len(x2))]) / sum(x2.volume)
                    row['timestamp'] = int(tm)
                    row['stock'] = stock
                    row['volume'] = sum(x2.volume)
                    newdf = newdf.append(row, ignore_index=True)
                    continue
            else:
                newdf = newdf.append(x)

        # Convert epoch to datetime and set it as index
        div = 1000
        newdf['timestamp'] = newdf.timestamp.apply(lambda ts: dt.datetime.fromtimestamp(ts / div))
        newdf.set_index('timestamp', inplace=True, drop=False)
        newdf.index.name = None

        # This needs to be an argument to the method
        # Do the resampling, forward fill price, sum the volume and leave unfilled as 0's
        fdf = pd.DataFrame()
        for stock in newdf.stock.unique():
            # fqstock = fq[fq.rirstquote_trades==stock]
            x2 = newdf[newdf.stock == stock]
            newerdf = x2.resample(self.delt).asfreq()
            newerdf = x2[['price']].resample(self.delt).mean().ffill()
            newerdf['volume'] = x2[['volume']].resample(self.delt).sum()
            newerdf['timestamp'] = newerdf.index
            newerdf['stock'] = stock
            # newerdf['delta_p'] =
            fdf = fdf.append(newerdf)
        return fdf


class ProcessData:
    """
    Process data from the websocket and get it ready to write to file or db.
    Process the files saved from websocket and deliver in requested form.
    """
    previousTimestamps = []
    testData = []
    cq = None
    tickers = []
    missing = []

    def __init__(self, tickers, fq=None, delt=None):
        """
        Paramaters
        ----------
        :params tickers: list<str> List of stocks
        :params fq: Firstquote, int. If int, represents unixtime to create Firstquote object
        :delt: timedelta: This is the sample rate for visualize data aggregation
        """
        self.tickers = tickers
        if fq:
            self.setFirstquote(fq)
            # self.cq = copy.deepcopy(fq)
            # self.fq = fq
            # self.missing.extend(set(self.tickers) - set(self.cq['firstquote_trades'].keys()))
        self.delt = delt

    def initializeReport(self, argdict):
        self.report = "report.json"
        self.report = formatFn(self.report, "json")
        self.begin = time.perf_counter()

        ddelt = 0 if not argdict['delt'] else argdict['delt'].microseconds
        fdata = {"numstocks": len(argdict['tickers']),
                 'delta': ddelt,
                 'store': argdict['store'],
                 'fill': argdict['ffill'],
                 'begin': 0}
        with open(self.report, 'w') as f:
            f.write(json.dumps(fdata))

    def addToReport(self, df):
        tbegin = util.unix2date(df.iloc[0]['timestamp'], unit='m')
        tend = util.unix2date(df.iloc[-1]['timestamp'], unit='m')
        elapsed = time.perf_counter() - self.begin
        fdata = '\n' + json.dumps({"tbegin": tbegin.strftime("%y/%m/%d %H:%M:%S.%f"),
                                   "tend": tend.strftime("%y/%m/%d %H:%M:%S.%f"),
                                   "elapsed": elapsed})

        with open(self.report, 'a') as f:
            f.write(fdata)

        return elapsed < 180

    def setFirstquote(self, fq):
        """
        Explanation
        -----------
        Retrieve quotes <= fq. Then for the most return a the price current for each
        stock at fq.timestamp. If no trades were found for a stock, set 0
        """
        assert len(self.tickers)
        if isinstance(fq, int):
            fq = createFirstQuote(fq, AllquotesModel, stocks=self.tickers)
        firstquote_trades = {x.stock: [x.close, x.volume] for x in fq.firstquote_trades}
        if set(self.tickers) != set(firstquote_trades.keys()):
            x = set(self.tickers) - set(firstquote_trades.keys())
            if x:
                self.missing = list(x)
                logging.warning(f'WARNING: Missing information for requested stock(s): {x}')
                logging.warning(f'WARNING: Using a firstquote of 0.0 for: {x}. The first returned data will replace the 0.0')
            for missing in x:
                firstquote_trades[missing] = [0.0, 0]
        self.fq = {'timestamp': fq.timestamp, 'firstquote_trades': firstquote_trades}
        self.cq = copy.deepcopy(self.fq)

    def getKeepAlives(self, dirname=None):
        """
        Retrieve the valid files created from the web socket as json files. This is based
        on reading the first line and haveing the rightr keys
        """
        if dirname is None:
            dirname = getCsvDirectory()
        validfiles = []
        for x in os.listdir(dirname):
            x = os.path.join(dirname, x)
            if not os.path.isfile(x):
                continue
            with open(x, 'r') as f:
                line = f.readline()
                f.close()
                try:
                    line = json.loads(line)
                    if not {'price', 'stock', 'timestamp', 'volume'}.issubset(set(line.keys())):
                        continue
                except Exception:
                    continue
                validfiles.append(x)
        return validfiles

    def readRawData(self, infile):
        line = ' '
        ix = 0
        df = pd.DataFrame()
        with open(infile, 'r') as f:
            while line:
                line = f.readline()
                if line:
                    ldf = pd.DataFrame(json.loads(line))
                    if ix == 0:
                        # Verify we got the right data structure
                        assert set(['price', 'stock', 'timestamp', 'volume']) == set(ldf.columns)

                    df = df.append(ldf)
                    ix += 1
        return df

    def latestfile(self, fnpattern, dirname=None):
        """
        Retrive the 'greatest' alphanumeric file from {dirname} that matches the regex
        fnpattern and is a legit json from the WebSocket
        """
        if dirname is None:
            dirname = getCsvDirectory()
        latest = ''
        for x in os.listdir(dirname):
            if re.match(fnpattern, x):
                x = os.path.join(dirname, x)
                if not os.path.isfile(x):
                    continue
                with open(x, 'r') as f:
                    line = f.readline()
                    f.close()
                    try:
                        line = json.loads(line)
                        if not {'price', 'stock', 'timestamp', 'volume'}.issubset(set(line.keys())):
                            continue
                    except Exception:
                        continue
                if x > latest:
                    latest = x
        latest = os.path.normpath(latest) if latest else ''
        return latest

    def setIndextoTimestamp(self, df):
        div = 1000
        newdf = df.copy()
        newdf['timestamp'] = newdf.timestamp.apply(lambda ts: dt.datetime.fromtimestamp(ts / div))
        newdf.set_index('timestamp', inplace=True, drop=False)
        newdf.index.name = None

        return newdf

    def getUndupedList(self, df):
        """
        :params return: list<DataFrame> where each dataframe contains the trades of a single stock
            DataFrames are indexed by time and are guaranteed to have unique indeces
        """
        stocklist = []
        for stock in df.stock.unique():
            astock = df[df.stock == stock]
            if astock.index.is_unique:
                stocklist.append
                stocklist.append(astock[['price', 'volume', 'stock']])
                continue
            unduped = pd.DataFrame([{'timestamp': k,
                                     'price': (v.price * v.volume).sum() / v.volume.sum(),
                                     'volume': v.volume.mean(),
                                     'stock': stock}
                                    for k, v in astock.groupby(['timestamp'])])
            unduped.set_index('timestamp', inplace=True)
            unduped.index.name = None
            stocklist.append(unduped)
        return stocklist

    def resample(self, stocklist, fqt):
        """
        Explanation
        -----------
        Given a list of DataFrames, each represent the trades of 1 ticker, resample the time frequence as self.delt
        """
        for i in range(len(stocklist)):
            s1 = stocklist[i]
            rate = self.delt
            ndf = s1.resample(self.delt).asfreq()
            ndf = s1[['price']].resample(rate).mean().ffill()
            ndf['volume'] = s1[['volume']].resample(rate).sum()
            ndf['timestamp'] = ndf.index
            stock = s1.stock.unique()[0]
            ndf['stock'] = stock
            ndf['delta_p'] = (ndf['price'] - fqt[stock][0]) / fqt[stock][0]
            ndf['delta_t'] = (ndf['timestamp'] - pd.Timestamp(self.fq['timestamp'])).dt.total_seconds()

            ndf.index.name = None
            self.testme(ndf)
            ndf.reset_index(inplace=True)
            stocklist[i] = ndf
        return stocklist

    def testme(self, ndf):
        assert ndf.index.is_unique
        assert set([len(ndf[ndf.timestamp == ts]) for ts in ndf.timestamp.unique()]) == {1}
        assert len(ndf.stock.unique()) == 1

    def fillData(self, stocklist, fq):
        """
        This requires iterating every timestamp and checking every record -- very time consuming.
        Because of that it makes sense to add the deltas here -- double duty -- to avoid iterating
        thie thing twice. Note that the delta_v cannot be done until after all these additions are made. (or can it???)
        """

        fqt = self.fq['firstquote_trades']
        # assert set(fqt.keys()) == set(df.stock.unique())
        cq = copy.deepcopy(fqt)
        self.missing = [x[0] for x in cq.items() if x[1][0] == 0]
        # for stock in cq
        mintime = pd.Timestamp(2030, 1, 1, 0, 0)
        maxtime = pd.Timestamp(1970, 1, 1, 0, 0)

        for tdf in stocklist:
            tdf.sort_values(['timestamp'])
            mintime = min(tdf.iloc[0].timestamp, mintime)
            maxtime = max(tdf.iloc[-1].timestamp, maxtime)
        ret_df = pd.DataFrame()
        for df in stocklist:
            stock = df.stock.unique()[0]
            newdf = copy.deepcopy(df)
            currtime = mintime
            for i in range(int((maxtime-mintime)/self.delt)+1):
                df_s = df[df.timestamp == currtime]

                if df_s.empty:
                    nrow = {}
                    nrow['price'] = cq[stock][0]
                    nrow['timestamp'] = currtime
                    nrow['stock'] = stock
                    nrow['volume'] = 0

                    nrow['delta_p'] = 0 if float(fqt[stock][0]) == 0 else (float(nrow['price']) - float(fqt[stock][0])) / float(fqt[stock][0])

                    nrow['delta_t'] = (nrow['timestamp'] - pd.Timestamp(self.fq['timestamp'], unit='s')).total_seconds()

                    newdf = newdf.append(nrow, ignore_index=True)
                else:
                    if len(df_s) > 1:
                        logging.error("ERROR: duplicate record found which should have been aggregated.")
                    if stock in self.missing:
                        fqt[stock][0] = df_s['price']
                        self.missing.remove(stock)
                    cq[stock][0] = df_s['price'].unique()[0]
                currtime += self.delt
            ret_df = ret_df.append(newdf)

        return ret_df

    def visualizeData2(self, infile, fq, oputfile='visualize_out.json'):

        # Verify that it seems to be the right kind of data
        df = pd.DataFrame()
        if infile.endswith('json'):
            line = ' '
            ix = 0
            with open(infile, 'r') as f:
                while line:
                    line = f.readline()
                    if line:
                        ldf = pd.DataFrame(json.loads(line))
                        if ix == 0:
                            # Verify we got the right data structure
                            assert set(['price', 'stock', 'timestamp', 'volume']) == set(ldf.columns)

                        df = df.append(ldf)
                        ix += 1
        if not self.tickers:
            self.tickers = list(df.stock.unique())
        if infile.endswith('csv'):
            raise NotImplementedError
        df = self.resampleit(df)
        self.setFirstquote(fq)

        self.cq = copy.deepcopy(self.fq)

    def visualizeDataNew(self, infile, fq, outfile='visualize_out.json'):
        outfile = formatFn(outfile, 'json')
        tc1 = time.perf_counter()
        df = self.readRawData(infile)
        tc2 = time.perf_counter()
        print('reading:', tc2-tc1)

        df = self.setIndextoTimestamp(df)
        tc3 = time.perf_counter()
        print('indexing:', tc3-tc2)

        stocklist = self.getUndupedList(df)
        tc4 = time.perf_counter()
        print('unduped', tc4-tc3)

        df_wdups = pd.DataFrame()

        self.tickers = list(df.stock.unique())
        self.setFirstquote(fq)
        fqt = self.fq['firstquote_trades']
        tc5 = time.perf_counter()
        print('Firstquote create:', tc5-tc4)

        stocklist = self.resample(stocklist, fqt)
        tc6 = time.perf_counter()
        print('Resample:', tc6-tc5)

        df_wdups = self.fillData(stocklist, fq)
        tc7 = time.perf_counter()
        print('filldata:', tc7-tc6)

        # self.testme2(df_wdups)
        vdata = self.formatData(df_wdups, ['visualize'], fill=True)
        tc8 = time.perf_counter()
        print('Format data:', tc8-tc7)

        outfile = os.path.join(getCsvDirectory(), outfile)
        self.writeFile(vdata, outfile, ['visualize'])
        tc9 = time.perf_counter()
        print("Write file:", tc9 - tc8)
        return outfile

    def testme2(self, df):
        assert len(set([len(z.timestamp) for z in [df[df.stock == x] for x in df.stock.unique()]])) == 1
        slen = set([len(z) for z in [df[df.timestamp == x] for x in df.timestamp.unique()]])
        assert len(slen) == 1
        assert list(slen)[0] == len(df.stock.unique())
        daset = set([set(z.stock) == set(df.stock.unique()) for z in [df[df.timestamp == x] for x in df.timestamp.unique()]])
        assert len(daset) == 1
        assert list(daset)[0] is True

    def visualizeData(self, infile, fq, outfile='visualize_out.json'):
        """
        Explanation
        -----------
        Open the file created by the websocket and turn it into the visualize format

        Paramaters
        ----------
        :infile: str: The file written with thge websocket data
        :fq: [int, Firstquote]: The data to  use for delta info in the visualize data
            int is Unix timestamp -- Retrieve Firstquote from allquotes table
        :outfile: str: A file name to save this data. Supply only name not path. The path
            is determined by program and a curren timestamp will be added to the name
        :return: The file name

        Object Parameters
        -----------------
        :self.tickers: If self.tickers is None or 0, The first quote is created (or assumed to be
            correct if provided) for the stocks in the file
        :self.delt: A timedelta for interval of aggregation.

        """
        df = pd.DataFrame()
        if infile.endswith('json'):
            line = ' '
            ix = 0
            with open(infile, 'r') as f:
                while line:
                    line = f.readline()
                    if line:
                        ldf = pd.DataFrame(json.loads(line))
                        df = df.append(ldf)
                        ix += 1
        return
        if not self.tickers:
            self.tickers = list(df.stock.unique())
        if infile.endswith('csv'):
            raise NotImplementedError
        self.tickers = list(df.stock.unique())
        self.setFirstquote(fq)

        self.cq = copy.deepcopy(self.fq)

        df = self.resampleit_fill(df)
        outfile = formatFn(outfile, 'json')

        vdata = self.formatData(df, ['visualize'], fill=True)
        outfile = os.path.join(getCsvDirectory(), outfile)
        self.writeFile(vdata, outfile, ['visualize'])
        return outfile

    def resampleit(self, df):
        """
        Just resample the raw data. No fill and not deltas
        """
        # df = self.aggregate
        newdf = pd.DataFrame()
        df.sort_values(['timestamp'], inplace=True)
        for count, stock in enumerate(df.stock.unique()):

            trades = (df[df.stock == stock]).sort_values(['timestamp'])
            prevtime = ''
            justagregated = ''
            replacements = pd.DataFrame()
            for i, row in trades.iterrows():
                # Find and aggregate rows with identical timestamps into new dfs
                # store them in newddf because while looping over them
                if row.timestamp == justagregated:
                    continue
                if row.timestamp == prevtime:
                    agregateThis = df[df.timestamp == row.timestamp]
                    tempdf = agregateThis.iloc[0].copy()
                    tempdf.price = sum([x * y for x, y in zip(agregateThis['price'], agregateThis['volume'])]) / sum(agregateThis['volume'])
                    tempdf.volume = sum(agregateThis['volume'])
                    replacements = replacements.append(tempdf)
                    justagregated = row.timestamp
                prevtime = row.timestamp

            print(" now we replace the dups")
            for i, row in replacements.iterrows():
                # replaceme = df[(df.timestamp == row.timestamp) & (df.stock == row.stock)]
                pass

        for count, tm in enumerate(df.timestamp.unique()):
            # First have to consolodate trades with identical timestamps
            x = df[df.timestamp == tm]
            if len(x) > 1:
                for stock in x.stock.unique():
                    x2 = x[x.stock == stock]
                    row = {}
                    row['price'] = sum([x2.iloc[i].price * x2.iloc[i].volume for i in range(len(x2))]) / sum(x2.volume)
                    row['timestamp'] = int(tm)
                    row['stock'] = stock
                    row['volume'] = sum(x2.volume)
                    newdf = newdf.append(row, ignore_index=True)
                    continue
            else:
                newdf = newdf.append(x)

        # Convert epoch to datetime and set it as index
        div = 1000
        newdf['timestamp'] = newdf.timestamp.apply(lambda ts: dt.datetime.fromtimestamp(ts / div))
        newdf.set_index('timestamp', inplace=True, drop=False)
        newdf.index.name = None

        # This needs to be an argument to the method
        # Do the resampling, forward fill price, sum the volume and leave unfilled as 0's
        fdf = pd.DataFrame()
        for stock in newdf.stock.unique():
            # fqstock = fq[fq.rirstquote_trades==stock]
            x2 = newdf[newdf.stock == stock]
            newerdf = x2.resample(self.delt).asfreq()
            newerdf = x2[['price']].resample(self.delt).mean().ffill()
            newerdf['volume'] = x2[['volume']].resample(self.delt).sum()
            newerdf['timestamp'] = newerdf.index
            newerdf['stock'] = stock
            # newerdf['delta_p'] =
            fdf = fdf.append(newerdf)
        return fdf

    def resampleit_fill(self, df):
        # df = self.aggregate
        fqt = self.fq['firstquote_trades']

        # Convert epoch to datetime and set it as index
        div = 1000

        newdf = pd.DataFrame()
        for count, tm in enumerate(df.timestamp.unique()):
            tm_df = df[df.timestamp == tm]
            for i, stock in enumerate(tm_df.stock.unique()):

                # If re got a trade for one of the missing stocks, add it to firstquote (self.fq)
                if self.missing:
                    deleteme = []
                    for mis in self.missing:
                        if mis in tm_df.stock.unique():
                            deleteme.append(mis)
                            fqt[mis][0] = tm_df[tm_df.stock == mis].price.unique()[0]
                            self.fq['firstquote_trades'] = fqt
                    for dem in deleteme:
                        self.missing.remove(dem)

                # Consolodate trades with identical timestamps, The operations are identity ops for singles
                # self.cq = 'asdf'
                tick_df = tm_df[tm_df.stock == stock]
                row = {}
                row['price'] = sum([tick_df.iloc[i].price * tick_df.iloc[i].volume for i in range(len(tick_df))]) / sum(tick_df.volume)
                row['timestamp'] = int(tm)
                row['stock'] = stock
                row['volume'] = sum(tick_df.volume)
                row['delta_p'] = (row['price'] - fqt[stock][0]) / fqt[stock][0]
                row['delta_t'] = (row['timestamp'] - (self.fq['timestamp'] * div)) / div
                newdf = newdf.append(row, ignore_index=True)
                self.cq['firstquote_trades'][stock] = [row['price'], row['volume']]

            for addme in (set(self.tickers) - set(tm_df.stock)):
                row = {}
                row['stock'] = addme
                row['price'] = self.cq['firstquote_trades'][addme][0]

                row['timestamp'] = tm
                row['volume'] = 0
                row['delta_p'] = row['price'] - fqt[addme][0]
                row['delta_t'] = (tm - (self.fq['timestamp'] * div)) / div
                newdf = newdf.append(row, ignore_index=True)

        newdf['timestamp'] = newdf.timestamp.apply(lambda ts: dt.datetime.fromtimestamp(ts / div))
        newdf.set_index('timestamp', inplace=True, drop=False)
        newdf.index.name = None

        # This needs to be an argument to the method
        # Do the resampling, forward fill price, sum the volume and leave unfilled as 0's
        fdf = pd.DataFrame()
        for stock in newdf.stock.unique():
            tick_df = newdf[newdf.stock == stock]
            newerdf = tick_df.resample(self.delt).asfreq()
            newerdf = tick_df[['price']].resample(self.delt).mean().ffill()
            newerdf['volume'] = tick_df[['volume']].resample(self.delt).sum()
            newerdf['timestamp'] = newerdf.index
            newerdf['stock'] = stock
            newerdf['delta_p'] = tick_df[['delta_p']].resample(self.delt).mean().ffill()
            newerdf['delta_t'] = tick_df[['delta_t']].resample(self.delt).mean().ffill()
            fdf = fdf.append(newerdf)
        return fdf

    def formatData(self, df, store, fill=False):
        """
        Paramaters
        ----------
        :params df: DataFrame. If store includes visualize, must include the collumns ['timestamp', 'stock']
        :params store: list.
        """
        if 'visualize' in store:
            if df.empty:
                return ''
            df.sort_values(['timestamp', 'stock'], inplace=True)
            df.reset_index(inplace=True)

            for t in df.timestamp.unique():

                # Note that t is a numpy.datetime. int(t) converts to Epoch in ns.
                tick = df[df.timestamp == t]
                cols = ['stock', 'price', 'volume']
                if fill:
                    cols.extend(['delta_p', 'delta_t'])
                current = [{int(int(t) / 1000000): [dict(tick[cols].iloc[i]) for i in range(len(tick))]}]
                if self.previousTimestamps:
                    self.previousTimestamps.extend(current)
                else:
                    self.previousTimestamps = current
            # current = self.findDups2()
            prevv = [[x['stock'], 0] for x in list(self.previousTimestamps[0].items())[0][1]]
            for i, x in enumerate(self.previousTimestamps):
                for j, trade in enumerate(list(x.items())[0][1]):
                    trade['delta_v'] = trade['volume'] + prevv[j][1]
                    prevv[j][1] = trade['delta_v']
                    assert prevv[j][0] == trade['stock']

            return json.dumps(self.previousTimestamps)

        elif 'json' in store:
            if df.empty:
                return ''
            df = df[['price', 'stock', 'timestamp', 'volume']]
            return df.to_json() + '\n'
        elif 'csv' in store:
            if df.empty:
                return [[]]
            return df.to_csv(header=True)

    def findDups2(self):
        """
        Deprecated-- for near immediate removal after current goals for visualizeData
        Aggregate duplicate time stamps. The data is not duplicated, just out of order
        timestamps in the websocket stream.
        """
        pts = self.previousTimestamps
        dups = {}
        fixthese = []

        # dups values need to be [[dict...]] to enable appending a duplicate [dict...]
        # After aggregating, dups needs to be transformed back into pts
        # for next time
        # Making dups val a tuple to keep track of index (when dup found, delete one, aggregate the other)
        for i, dj in enumerate(pts):
            ts = list(dj.keys())[0]
            if dups.get(ts):
                dups[ts][0].append(dj[ts])
                print('duplicate', ts)
                fixthese.append((ts, (dups[ts][1], i)))
            else:
                dups[ts] = ([dj[ts]], i)
        d3final = {}
        if fixthese:
            for ts, (i, _) in fixthese:
                stocks = [z['stock'] for z in dups[ts][0][0]]
                d3final[ts] = []
                for stock in stocks:
                    d3 = {}
                    d1 = [z for z in dups[ts][0][0] if z['stock'] == stock][0]
                    d2 = [z for z in dups[ts][0][1] if z['stock'] == stock][0]
                    d3['stock'] = stock
                    d3['price'] = (d1['price'] * (d1['volume'] + 1) + (d2['price'] * (d2['volume'] + 1))) / (d1['volume'] + d2['volume'] + 2)
                    d3['volume'] = d1['volume'] + d2['volume']
                    d3['delta_p'] = (d1['delta_p'] + d2['delta_p']) / 2

                    #  delta_v is actually an accumulating sum, not a delta. Can't be figured till 'dups' are aggregated
                    # Note that the delta_t values are pre-resample and my vary within the sample rate
                    d3['delta_t'] = (d1['delta_t'] + d2['delta_t']) / 2
                    d3final[ts].append(d3)

                # Aggregate the dup into global list
                pts[i][ts] = d3final[ts]

            # Delete the other dup (backwards to retain ix location)
            for i in range(len(fixthese)-1, -1, -1):
                pts.pop(fixthese[i][1][1])
                # dups[ts] = d3final[ts]
            # self.previousTimestamps = [dups]
        prevv = [[x['stock'], 0] for x in list(pts[0].items())[0][1]]

        for i, x in enumerate(pts):
            for j, trade in enumerate(list(x.items())[0][1]):
                trade['delta_v'] = trade['volume'] + prevv[j][1]
                prevv[j][1] = trade['delta_v']
                assert prevv[j][0] == trade['stock']

        return json.dumps(pts)

    def writeFile(self, j, fn, store):
        # Completely rewriting the file with every addition -- for now it will reduce risk of error
        if 'visualize' in store:
            mode = 'w'
        else:
            mode = 'w' if not os.path.exists(fn) else 'a'

        if 'visualize' in store or 'json' in store:
            with open(fn, mode) as f:
                f.write(j)

        elif 'csv' in 'store':
            with open(fn, 'w', newline='') as f:
                writer = csv.writer(f)
                for row in j:
                    writer.writerow(row)


if __name__ == "__main__":
    ###########################################
    # [0.25, 1, 3, 7, 10]
    delt = dt.timedelta(seconds=10)
    procd = ProcessData([], None, delt)
    fn = os.path.normpath(r"C:\python\E\uw\quotedb\data\prod\accumulate_40__20210506_123254.json")
    fq = util.dt2unix_ny(dt.datetime(2021, 4, 22, 9, 30))
    outfile = "zavizuel_.json"
    procd.visualizeDataNew(fn, fq)

    ###########################################
    # fn = formatFn("/testfile_csvasdf.csv", format='json')
    # # resample_td = dt.timedelta(seconds=0.25)
    # resample_td = dt.timedelta(seconds=0.25)
    # store = ['visualize']
    # stocks = ['AAPL', 'ROKU', 'TSLA', 'INTC', 'CCL', 'VIAC', 'ZKIN', 'AMD']
    # stocks.append("BINANCE:BTCUSDT")

    # d = util.dt2unix_ny(dt.datetime(2021, 4, 1, 15, 30))
    # fq = createFirstQuote(d, AllquotesModel, stocks=stocks, usecache=True)
    # mws = MyWebSocket(stocks, fn, store=store, resample_td=resample_td, fq=fq, ffill=True)
    # mws.start()
    # while True:
    #     if not mws.is_alive():
    #         print('Websocket was stopped: restarting...')
    #         mws = MyWebSocket(stocks, fn, store=store, resample_td=resample_td, fq=fq, ffill=True)

    #         mws.start()
    #     time.sleep(20)
    #     print(' ** ')
    ######################################################
    # d = util.dt2unix_ny(dt.datetime(2021, 4, 1, 15, 30))
    # stocks = ['AAPL', 'ROKU', 'TSLA', 'INTC', 'CCL', 'VIAC', 'ZKIN', 'AMD']
    # stocks.append("BINANCE:BTCUSDT")
    # fn = 'bubblething.json'
    # store = ['csv']
    # fq = createFirstQuote(d, AllquotesModel, stocks=stocks, usecache=True)
    # mws = MyWebSocket(stocks, fn, store=store, fq=fq)
