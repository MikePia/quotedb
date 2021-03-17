import csv
import json
import sys
import time
import threading
import websocket
from models.trademodel import ManageTrade, TradeModel
from stockdata.sp500 import random50
from stockdata.dbconnection import getFhToken, getCsvDirectory, getSaConn


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
    """

    def __init__(self, tickers, fn, store=['csv']):
        threading.Thread.__init__(MyWebSocket)
        self.tickers = tickers
        self.fn = fn
        self.store = store
        self.daemon = True

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
                # print('.', end='')
            elif 'json' in self.store:
                with open(self.fn, 'a') as f:
                    f.write(json.dumps(j))
                    # print(f'Wrote {len(j["data"])} trades to file {self.fn}')
                    print('.', end='')
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
        unsubscribe = set(self.tickers) - set(newstocks)
        subscribe = set(newstocks) - set(self.tickers)
        if unsubscribe:
            self.unsubscribe(unsubscribe)
        if subscribe:
            self.subscribe(subscribe)
        self.tickers = newstocks


if __name__ == "__main__":
    ###########################################
    # stocks = nasdaq100symbols
    # fn = getCsvDirectory() + "/testfile.json"
    # mws = MyWebSocket(stocks, fn, store=['db'])
    #############################################

    group1 = random50()
    group2 = random50()
    # stocks = ['SILLY',  'AAPL', 'SQ', 'ROKU', 'TSLA', 'BINANCE:BTCUSDT']
    fn = getCsvDirectory() + "/testfile.csv"
    mws = MyWebSocket(group1, fn, ['csv'])
    mws.start()
    print('\n============================= SLEEP =============================\n')
    time.sleep(10)
    print('\n=============================================================')
    print('=============================================================')
    print('=============================================================')
    print('=============================================================')
    mws.changesubscription(group2, newfn=getCsvDirectory() + '/newtestfile.csv')
    time.sleep(10)
    print('about to quit')
    sys.exit()

    print('done')
