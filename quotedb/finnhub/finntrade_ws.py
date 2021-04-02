import csv
import datetime as dt
import json
import pandas as pd
import threading
import time
import websocket
from quotedb.models.trademodel import ManageTrade, TradeModel
from quotedb.dbconnection import getFhToken, getCsvDirectory, getSaConn
from quotedb.utils.util import formatData, writeFile


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
    :params delt: timedelta: If it is not node, aggreagate the results by the resample amount.

    Programming notes TODO
    ----------------------
    retool csv, json, and visualize to use the resampled result if it is there. I don't think
    we need to store to  databse with resampled data.
    """

    def __init__(self, tickers, fn, store=['csv'], delt=None):
        threading.Thread.__init__(MyWebSocket)
        self.tickers = tickers
        self.delt = delt
        self.fn = fn
        self.store = store
        self.daemon = True
        self.aggregate = pd.DataFrame()
        self.aggmin = 0
        self.aggmax = 0

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
                # Gathere togethere at least 5 secondes of data to aggregate
                self.aggregate = self.aggregate.append(df)
                times = list(self.aggregate.timestamp)
                if (max(times) - min(times)) >= 5000:
                    newdata = self.resampleit()
                    self.aggregate = pd.DataFrame()
                    print('New data to deal with here ...', len(newdata))
                else:
                    return
            if 'csv' in self.store:
                trades = [[t['s'], t['p'], t['t'], t['v']] for t in j['data']]
                with open(self.fn, 'a', newline='') as f:
                    csvwriter = csv.writer(f)
                    for trade in trades:
                        csvwriter.writerow(trade)
                # Oversharaing
                # print(f'Added {sum([x[3] for x in trades])} shares in', {x[0] for x in trades})
                print('.', end='')
                # pprint(f'Wrote {len(trades)} trades to {self.fn}')
            elif 'json' in self.store:
                with open(self.fn, 'a') as f:
                    f.write(json.dumps(j))
                    # print(f'Wrote {len(j["data"])} trades to file {self.fn}')
                    print('.', end='')
            elif 'visualize' in self.store:
                df = pd.DataFrame([[t['s'], t['p'], t['t'], t['v']] for t in j['data']])
                print('.', end='')
                writeFile(formatData(df, self.store), self.fn, self.store)
            elif 'dev' in self.store:
                print('.', end='')
                # self.store.append('visualize')
                writeFile(formatData(newdata, ['visualize']), self.fn, 'visualize')

            if 'db' in self.store:
                TradeModel.addTrades(j['data'], self.mt.engine)

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
            x2 = newdf[newdf.stock == stock]
            newerdf = x2.resample(self.delt).asfreq()
            newerdf = x2[['price']].resample(self.delt).mean().ffill()
            newerdf['volume'] = x2[['volume']].resample(self.delt).sum()
            newerdf['timestamp'] = newerdf.index
            newerdf['stock'] = stock
            fdf = fdf.append(newerdf)
        return fdf


if __name__ == "__main__":
    ###########################################
    from quotedb.sp500 import nasdaq100symbols
    stocks = nasdaq100symbols
    stocks.append("BINANCE:BTCUSDT")
    stocks.append("IC MARKETS:1")
    fn = getCsvDirectory() + "/testfile.json"
    delt = dt.timedelta(seconds=0.25)
    # delt = None
    store = ['dev']
    # store = ['visualize']
    mws = MyWebSocket(stocks, fn, store=store, delt=delt)
    mws.start()
    while True:
        if not mws.is_alive():
            print('Websocket was stopped: restarting...')
            mws = MyWebSocket(stocks, fn, store=store, delt=delt)

            mws.start()
        time.sleep(20)
        print(' ** ')

    #############################################

    # group1 = list(random50())
    # group2 = list(random50())
    # group1.append("BINANCE:BTCUSDT")
    # group1.append("IC MARKETS:1")
    # group2.append("BINANCE:BTCUSDT")
    # group2.append("IC MARKETS:1")
    # # stocks = ['SILLY',  'AAPL', 'SQ', 'ROKU', 'TSLA', 'BINANCE:BTCUSDT']
    # fn = getCsvDirectory() + "/testfile.csv"
    # mws = MyWebSocket(group1, fn, ['csv'])
    # mws.start()
    # print('\n============================= SLEEP =============================\n')
    # time.sleep(10)
    # print('\n=============================================================')
    # print('=============================================================')
    # print('=============================================================')
    # print('=============================================================')
    # mws.changesubscription(group2, newfn=getCsvDirectory() + '/newtestfile.csv')
    # time.sleep(10)
    # print('about to quit')
    # sys.exit()

    # print('done')
