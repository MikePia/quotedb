# https://pypi.org/project/websocket_client/
# https://github.com/websocket-client/websocket-client/issues/469

import json
import logging
import websocket
from stockdata.dbconnection import getFhToken, getSaConn
from models.trademodel import TradeModel, ManageTrade
from threading import Thread


class MyWebSocket():
    counter = 0
    bulk = []
    NUMREC = 15
    def __init__(self, arr, url=f"wss://ws.finnhub.io?token={getFhToken()}"):
        print('Creating websocket...')
        self.arr = arr
        self.mt = ManageTrade(getSaConn())
        self.ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={getFhToken()}",
                                on_message = self.on_message,
                                on_error = self.on_error,
                                on_close = self.on_close)
        self.ws.on_open = self.on_open

        self.wst = Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        # self.ws.run_forever()

    def close(self):
        self.ws.close()

    def on_message(self, message):
        print('==========================================================================')
        j = json.loads(message)
        if j['type'] != 'trade':
            logging.info("Retrieved something non-standard frm trades endpoint:")
            logging.info(j)
        else:
            self.bulk.extend(j['data'])
            self.counter += 1
        if self.counter == self.NUMREC:
            self.counter = 0
            TradeModel.addTrades(self.bulk, self.mt.engine)
            self.bulk = []
        

    def on_error(self, error):
        print(error)

    def on_close(self):
        print("### closed ###")

    def on_open(self):
        for ticker in self.arr:
            msg = f'{{"type":"subscribe","symbol":"{ticker}"}}'
            self.ws.send(msg)
        # XX = '{"type":"subscribe","symbol":"AAPL"}'

        # msg2 = '{"type":"subscribe","symbol":"AMZN"}'
        # msg3 = '{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}'
        # msg4 = '{"type":"subscribe","symbol":"IC MARKETS:1"}'
        # self.ws.send(msg2)
        # self.ws.send(msg3)
        # self.ws.send(msg4)
        # for ticker in arr:
        #     self.ws.send(f'{{"type":"subscribe","symbol":"{ticker}"}}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    # Note: to get results on off hours using bit coin
    mws = MyWebSocket(['AAPL', 'AMZN', 'ROKU', 'GME', 'TSLA', 'BB', 'SQ', 'MU', 'BINANCE:BTCUSDT'])
    print('did it wait?')
    # ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={getFhToken()}",
    #                             on_message = on_message,
    #                             on_error = on_error,
    #                             on_close = on_close)
    # ws.on_open = on_open
    # ws.run_forever()