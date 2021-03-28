import websocket
from quotedb.dbconnection import getFhToken


def on_message(ws, message):
    print(message)


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")


def on_open(ws):
    arr = ["AAPL", "AMZN", "BINANCE:BTCUSDT", "IC MARKETS:1"]
    for t in arr:
        msg = f'{{"type":"subscribe","symbol":"{t}"}}'
        ws.send(msg)


if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={getFhToken()}",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
