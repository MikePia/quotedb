import datetime as dt
import pandas as pd
import requests
from models.polytrademodel import PolyTradeModel, ManagePolyTrade
from stockdata.dbconnection import getPolygonToken, getSaConn, getCsvDirectory
from qexceptions.qexception import InvalidServerResponseException
from utils.util import unix2date, dt2unix
from stockdata.sp500 import random50


class PolygonApi:

    BASEURL = "https://api.polygon.io"
    # /v2/ticks/stocks/trades/AAPL/2020-10-14?reverse=true&limit=5000&apiKey=FF3L2jIzaz621a5yNwdsA7FWhkRZcO3z"

    TRADES = BASEURL + "/v2/ticks/stocks/trades/{ticker}/{date}"
    mpt = ManagePolyTrade(getSaConn())

    def getTrades(self, ticker, date, reverse='false', limit=50000, offset=0):
        url = self.TRADES.format(ticker=ticker, date=date)
        params={}
        params['reverse'] = reverse
        params['limit']=limit
        params['apiKey'] = getPolygonToken()
        params['timestamp'] = offset
        response = requests.get(url, params=params)
        status = response.status_code
        if status != 200:
            raise InvalidServerResponseException(f'Server returned status {status}:{response.text}')
        return response.json()

    def getAllTradesOnDay(self, ticker, date, reverse='false', limit=50000):
        offset = 0
        total = []
        while True:
            j = self.getTrades(ticker, date, reverse, limit, offset)
            offset = j['results'][-1]['t']
            getActualTime(j['results'][0]['t'])
            getActualTime(j['results'][-1]['t'])
            if not j:
                break

            # convert Time in nanoseconds to seconds. If we decide to resample, do this in pandas- more effecient
            for t in j['results']:
                t['t'] = t['t'] / 1000000000
            total.extend(j['results'])
            getActualTime(total[-1]['t'])   # t, y, and f are all times, y:generated, 
                                            # t:exchange Produced it, f:Reprot facitlity received
                                            # t seems right as it's the time of commercial viability
            if len(j['results']) < limit:
                # We've come to the end of the data
                break
        
        # mpt = ManagePolyTrade(getSaConn())
        PolyTradeModel.addTrades(ticker, total, self.mpt.engine)
        return total

    def getTradeRange(self, ticker, date, start, stop=None, reverse='false', limit=50000):
        '''
        Get an intraday range of Trades
        :params date: datetime.date, The day to retrieve trades from
        :params start: datetime.datetime, The precise datetime to start retrieving
        :params stop: datetime.datetime, Thie end datetime to retrieve or, if None, the end of the data for the day
        '''
        offset = dt2unix(start, unit='n')
        total = []
        if stop == None:
            stop = dt.datetime(date.year, date.month, date.day, 23, 59, 59)
        stop = dt2unix(stop, unit='n')
        while True:
            j = self.getTrades(ticker, date.strftime("%Y-%m-%d"), reverse, limit, offset)
            if not j or not j['results']:
                break
            offset = j['results'][-1]['t']

            total.extend(j['results'])
            if len(j['results']) < limit or offset >= stop:
                # We've come to the end of the data or the end of the requested data.
                break
        
        if not total:
            return None, -1
        df = self.resampleit(total, pd.Timedelta(seconds=0.25))
        # mpt = ManagePolyTrade(getSaConn())
        # PolyTradeModel.addTrades(ticker, total, mpt.engine)
        PolyTradeModel.addTradesFromDf(ticker, df, self.mpt.engine)
        # getTimeDifferences([x['t'] for x in total])
        lasttime = total[-1]['t']
        return df, lasttime

    def cycleStocksToCurrent(self, tickers, adate,  start, beginmin=False):
        '''
        Generally adate should be today. 
        :params adate: date
        :params start: datetime needs to be a datetime that occurs on same day as adate
        '''
        self.cycle = {k:0 for k in tickers}
        if beginmin:
            for k, v in self.mpt.getMaxTimeForEachTicker(tickers=tickers).items():
                self.cycle[k] = v
        while True:
            for i, tick in enumerate(tickers):
                if self.cycle[tick] > 0:
                    start = unix2date(self.cycle[tick], unit='n')
                df, lasttime = self.getTradeRange(tick, adate, start)
                if df is None:
                    continue
                self.cycle[tick] = lasttime+1
                
            print('\n========================= Completed a cycle=========================\n')

    def resampleit(self, j, delt):
        df = pd.DataFrame(j)[['t', 's', 'p']]
        df.rename(columns={'t': 'time', 's': 'volume', 'p': 'price'}, inplace=True)
        # return df
        df.time = df.time.apply(lambda ts: pd.Timestamp(ts, unit='ns'))
        df.set_index('time', inplace=True, drop=False)
        df = df.resample(delt).agg({'price': 'mean', 'volume': 'sum', 'time': 'first'}).asfreq(delt)
        epoch = dt.datetime.utcfromtimestamp(0)
        df.time = df.index
        df.volume = df.volume.fillna(0)
        df.price = df.price.fillna(method='ffill')
        epoch = dt.datetime.fromtimestamp(0)
        df.time = df.time.apply(lambda ts: dt2unix(ts, unit='n'))
        return df
        
def getTimeDifferences(times):
    diffs = {}
    prev = 0
    for t in times:
        diff = t-prev
        if not diffs.get(diff):
            diffs[diff] =  1
        else:
            diffs[diff] += 1
        prev = t
    for k, v, in diffs.items():
        print(f'for {k/1000000000} seconds, there are {v} values at {getActualTime(t)}')

def getActualTime(timestamp_nano):
    d = dt.datetime.fromtimestamp(timestamp_nano/1000000000)
    print(d.strftime('%A %B %d %H:%M:%S'))

if __name__ == '__main__':
    # Server is on Berlin time, hmmm
    nydiff = 6
    pa =  PolygonApi()
    # ticker = 'DLTR'
    tttdate = pd.Timestamp.today()
    start = pd.Timestamp.today() - pd.Timedelta(hours=3 + nydiff)
    tdate = dt.date(start.year, start.month, start.day)
    print(start)
    pa.cycleStocksToCurrent(random50(numstocks=50), tdate, start, beginmin=True)
    # # pa.cycleStocksToCurrent(['FISV'], tdate, start)
    # print()


