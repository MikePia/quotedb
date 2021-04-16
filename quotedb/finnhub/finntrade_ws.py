import copy
import csv
import datetime as dt
import json
import logging
import os
import pandas as pd
import threading
import websocket
from quotedb.dbconnection import getFhToken, getSaConn
from quotedb.dbconnection import getCsvDirectory
from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.common import createFirstQuote

from quotedb.models.trademodel import ManageTrade, TradeModel
from quotedb.utils.util import formatData, writeFile, dt2unix_ny


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
        self.fq = None
        self.cq = None
        if fq:
            # We need to be able to select these by stock. Dataframe or dict? go dict
            # Have to also change the type of the enclosing fq, it has a relationship with firstquote_trades
            # Also changing to 'trade' format (matches ws data)  no high, low or open and price = close
            # result is {'timestamp': <int>, 'firstquote_trades': <DataFrame>}
            firstquote_trades = {x.stock: [x.close, x.volume] for x in fq.firstquote_trades}
            # firstquote_trades = pd.DataFrame([(x.stock, x.close, x.volume) for x in fq.firstquote_trades], columns=cols)
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
                if (max(times) - min(times)) >= 5000:
                    if self.ffill:
                        df = self.resampleit_fill()
                    else:
                        df = self.resampleit()
                    self.aggregate = pd.DataFrame()
                    print('New data to deal with here ...', len(df))
                else:
                    return
            if 'json' in self.store or 'visualize' in self.store or 'csv' in self.store:
                print('.', end='')
                writeFile(formatData(df, self.store, self.ffill), self.fn, self.store)
            if 'db' in self.store:
                TradeModel.addTrades(df, self.mt.engine)

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

    def resampleit_fill(self):
        df = self.aggregate
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
                row['delta_v'] = row['volume'] - fqt[stock][1]
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
                row['delta_v'] = -fqt[addme][1]
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
            newerdf['delta_v'] = tick_df[['delta_v']].resample(self.delt).mean().ffill()
            newerdf['delta_t'] = tick_df[['delta_t']].resample(self.delt).mean().ffill()
            fdf = fdf.append(newerdf)
        return fdf


class FormatData:
    previousTimestamps = []

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
            visualize = []

            for t in df.timestamp.unique():
                # Note that t is a numpy.datetime. int(t) converts to Epoch in ns.
                tick = df[df.timestamp == t]
                cols = ['stock', 'price', 'volume']
                if fill:
                    cols.extend(['delta_p', 'delta_t', 'delta_v'])

                visualize.append({json.dumps(int(int(t) / 1000000)): tick[cols].to_json(orient="records")})
            self.writeFile = None
            if self.previousTimestamps:
                mincurrent = min([list(x.keys())[0] for x in visualize])
                maxprev = max([list(x.keys())[0] for x in self.previousTimestamps])
                if mincurrent <= maxprev:
                    visualize = self.aggregateNewOld(visualize, mincurrent)
                    self.previousTimestamps = visualize
                    self.setWriteMode = 'w'
                else:
                    self.previousTimestamps.extend(visualize)
            else:
                self.previousTimestamps.extend(visualize)
            return json.dumps(visualize, separators=(',', ':')).replace('"[', '[').replace(']"', ']').replace("\\", "")[1:-1]

        elif 'json' in store:
            if df.empty:
                return ''
            return df.to_json()
        elif 'csv' in store:
            if df.empty:
                return [[]]
            return df.to_csv(header=True)

    def _bracketdata(self, fn, content):
        with open(fn, 'w') as f:
            f.write('[' + content + ']')

    def aggregateNewOld(self, visualize, mincurrent):
        newts = set([list(x.keys())[0] for x in visualize])
        oldts = set([list(x.keys())[0] for x in self.previousTimestamps])
        overlap = newts.intersection(oldts)
        aggregate = []
        for ts in overlap:
            for vis in visualize:
                if list(vis.keys())[0] == ts:
                    visdict = vis
                    break
            for vis in self.previousTimestamps:
                if list(vis.keys())[0] == ts:
                    prevdict = vis
            newdict = {'asdf': []}
            print()
            

        return self.previousTimestamps

    def _replaceLastBracket(self, fn, newcontent):
        '''
        This relies on the file having no extra spaces at the end.
        We rely on being the only editor of the file to sort of guarantee it.
        '''
        cursize = os.path.getsize(fn)
        with open(fn, 'r+') as f:
            begchar = f.readline().strip()[0]
            f.seek(cursize-1)
            lastchar = f.read().strip()[-1]
            if begchar == '[' and lastchar == ']':
                f.seek(cursize-1)
                f.write(',' + newcontent + ']')
            else:
                raise ValueError('File is in bad state, nothing appended')

    def writeFile(self, j, fn, store):
        mode = 'a' if (os.path.exists(fn) and os.path.getsize(fn) > 0) else 'w'
        if 'visualize' in store or 'json' in store:

            if mode == 'a':
                self._replaceLastBracket(fn, j)
            else:
                self._bracketdata(fn, j)
        elif 'csv' in 'store':
            with open(fn, mode, newline='') as f:
                writer = csv.writer(f)
                for row in j:
                    writer.writerow(row)


if __name__ == "__main__":
    ###########################################
    import time
    from quotedb.utils.util import formatFn
    fn = formatFn("/testfile_csvasdf.csv", format='json')
    # # resample_td = dt.timedelta(seconds=0.25)
    resample_td = dt.timedelta(seconds=0.25)
    store = ['visualize']
    stocks = ['AAPL', 'ROKU', 'TSLA', 'INTC', 'CCL', 'VIAC', 'ZKIN', 'AMD']
    stocks.append("BINANCE:BTCUSDT")

    d = dt2unix_ny(dt.datetime(2021, 4, 1, 15, 30))
    fq = createFirstQuote(d, AllquotesModel, stocks=stocks, usecache=True)
    mws = MyWebSocket(stocks, fn, store=store, resample_td=resample_td, fq=fq, ffill=True)
    mws.start()
    while True:
        if not mws.is_alive():
            print('Websocket was stopped: restarting...')
            mws = MyWebSocket(stocks, fn, store=store, resample_td=resample_td, fq=fq, ffill=True)

            mws.start()
        time.sleep(20)
        print(' ** ')
    ######################################################
    # d = dt2unix_ny(dt.datetime(2021, 4, 1, 15, 30))
    # stocks = ['AAPL', 'ROKU', 'TSLA', 'INTC', 'CCL', 'VIAC', 'ZKIN', 'AMD']
    # stocks.append("BINANCE:BTCUSDT")
    # fn = 'bubblething.json'
    # store = ['csv']
    # fq = createFirstQuote(d, AllquotesModel, stocks=stocks, usecache=True)
    # mws = MyWebSocket(stocks, fn, store=store, fq=fq)
