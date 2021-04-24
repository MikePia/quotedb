import datetime as dt
import time
from quotedb.utils import util

from quotedb import sp500
from quotedb import getdata as gd
from quotedb.models import metamod as mm


# stocks = ['CERN', 'CSCO', 'GILD', 'KDP', 'MAR', 'MU', 'AAPL']
def runit():
    for i in range(5, 31, 5):
        stocks = sp500.random50(numstocks=i)
        # stocks.append('BINANCE:BTCUSDT')
        # delt = dt.timedelta(seconds=10)
        fn = f"x_{len(stocks)}_report_json.json"
        fn = util.formatFn(fn, 'json')

        # fq = util.dt2unix_ny(dt.datetime(2021, 4, 22, 9, 30))
        # gd.startTickWS_SampleFill(stocks, fn, fq, delt=delt)
        store = ['json']
        gd.startTickWSKeepAlive(stocks, fn, store, delt=None,)
        mm.cleanup()
        mm.init()
        print("sleeping for 30")
        time.sleep(30)


runit()


# begin = time.perf_counter()
# gd.startTickWSKeepAlive(stocks, fn, store=['json'], delt=None, polltime=5)
