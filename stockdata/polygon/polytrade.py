import logging
import datetime as dt
import pandas as pd
import requests
from models.polytrademodel import PolyTradeModel, ManagePolyTrade
from models.holidaymodel import HolidayModel, ManageHolidayModel
from stockdata.dbconnection import getPolygonToken, getSaConn
from qexceptions.qexception import InvalidServerResponseException
from utils.util import dt2unix


class PolygonApi:
    """
    """

    BASEURL = "https://api.polygon.io"
    TRADES = BASEURL + "/v2/ticks/stocks/trades/{ticker}/{date}"

    mpt = ManagePolyTrade(getSaConn())
    holman = ManageHolidayModel(getSaConn())

    def __init__(self, tickers, begdate, start=0,  resamprate=pd.Timedelta(seconds=0.25), filternull=False, timer=None):
        if timer:
            if isinstance(timer, dt.datetime):
                self.timer = timer
            elif isinstance(timer, dt.timedelta):
                self.timer = pd.Timestamp.now() + timer
            else:
                raise ValueError('Illegal type. Must be either datetitme, timedelta or None')
        else:
            self.timer = None
        self.now = dt.datetime.utcnow().date()
        self.begdate = begdate
        self.rate = resamprate
        self.tickers = tickers
        self.cycle = {k: [start, self.begdate] for k in tickers}
        self.filternull = filternull

    def getTrades(self, ticker, date, reverse='false', limit=50000, offset=0):
        url = self.TRADES.format(ticker=ticker, date=date)
        params = {}
        params['reverse'] = reverse
        params['limit'] = limit
        params['apiKey'] = getPolygonToken()
        params['timestamp'] = offset
        response = requests.get(url, params=params)
        status = response.status_code
        if status != 200:
            logging.error("server error while trying", response.url)
            raise InvalidServerResponseException(f'Server returned status {status}:{response.text} {response.url}')
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

            # t, y, and f are all times, y:generated,
            # t:exchange Produced it, f:Reprot facitlity received
            # t seems right as it's the time of commercial viability
            getActualTime(total[-1]['t'])
            if len(j['results']) < limit:
                # We've come to the end of the data
                break

        # mpt = ManagePolyTrade(getSaConn())
        PolyTradeModel.addTrades(ticker, total, self.mpt.engine)
        return total

    def getTradeRange(self, ticker, stop=None, reverse='false', limit=50000):
        '''
        Get an intraday range of Trades
        :params date: datetime.date, The day to retrieve trades from
        :params stop: datetime.datetime, Thie end datetime to retrieve or, if None, the end of the data for the day
        '''
        offset = self.cycle[ticker][0]
        date = self.cycle[ticker][1]

        offset = offset if offset else 0

        total = []
        if stop is None:
            stop = dt.datetime(date.year, date.month, date.day, 23, 59, 59)
        stop = dt2unix(stop, unit='n')
        while True:
            j = self.getTrades(ticker, date.strftime("%Y-%m-%d"), reverse, limit, offset)
            if not j or not j['results']:
                break
            if j['results'][0]['t'] <= offset:
                print('We need to fix this now')
            assert j['results'][-1]['t'] > offset
            offset = j['results'][-1]['t'] + 1

            self.cycle[ticker][0] = max(self.cycle[ticker][0], offset)

            total.extend(j['results'])
            if len(j['results']) < limit or offset >= stop:
                # Go to the next day until we get to the current day.
                # Skip Weekends and Known holidays to avoid 404s
                # We've come to the end of the day or end of data we are at the bleeding edge of data
                if self.cycle[ticker][1] < self.now:
                    self.cycle[ticker][0] = offset = 0

                    nextbiz = self.nextBizDay(self.cycle[ticker][1])
                    if nextbiz <= self.now:
                        self.cycle[ticker][1] = date = nextbiz
                    else:
                        break
                else:
                    break

        if not total:
            return None, -1
        df = self.resampleit(total, self.rate, filternull=self.filternull)
        PolyTradeModel.addTradesFromDf(ticker, df, self.mpt.engine)
        return df

    def cycleStocksToCurrent(self, beginmin=False):
        '''

        Cycle through stocks and add tem to the db`
        :params start: datetime needs to be a datetime that occurs on same day as adate
        '''
        if beginmin:
            for k, v in self.mpt.getMaxTimeForEachTicker(tickers=self.tickers).items():
                begdate = pd.Timestamp(v, tz="US/Eastern").date()
                self.cycle[k] = [v+1, begdate]
        while True:
            for i, tick in enumerate(self.tickers):
                df = self.getTradeRange(tick)
                if df is None:
                    raise ValueError("Programmers Raise. What to do here?")
                if self.cycle[tick][1] == self.now:
                    self.cycle[tick][0] = PolyTradeModel.getMaxTime(tick, self.mpt.engine)

            print(f'\n=================== Completed a cycle of {len(self.tickers)} stocks =========================\n')

    def nextBizDay(self, d):
        '''
        Get the next market day from d
        :params d: date
        '''
        if HolidayModel.isHoliday(d, self.holman.session):
            d += dt.timedelta(days=1)
        days = 1 if d.weekday() < 4 else (7 - d.weekday())
        d += dt.timedelta(days=days)
        if HolidayModel.isHoliday(d, self.holman.session):
            d += dt.timedelta(days=1)

        return d

    def resampleit(self, j, delt, filternull=False):
        df = pd.DataFrame(j)[['t', 's', 'p']]
        df.rename(columns={'t': 'time', 's': 'volume', 'p': 'price'}, inplace=True)
        # return df
        df.time = df.time.apply(lambda ts: pd.Timestamp(ts, unit='ns'))
        df.set_index('time', inplace=True, drop=False)
        df = df.resample(delt).agg({'price': 'mean', 'volume': 'sum', 'time': 'first'}).asfreq(delt)
        # epoch = dt.datetime.utcfromtimestamp(0)
        df.time = df.index
        df.volume = df.volume.fillna(0)
        df.price = df.price.fillna(method='ffill')
        # epoch = dt.datetime.fromtimestamp(0)
        if filternull:
            df = df[df.volume != 0]
        df.time = df.time.apply(lambda ts: dt2unix(ts, unit='n'))
        return df

def isMarketOpen():
    url = f'https://api.polygon.io/v1/marketstatus/now?&apiKey={getPolygonToken()}'
    RETRIES = 5
    while RETRIES > 0:
        response = requests.get(url)
        if response.status_code != 200:
            logging.error("Server error while trying", response.url)
            if RETRIES == 0:
                return {}
            RETRIES -= 1

            continue
        RETRIES = 0
    return response.json()['exchanges']['nasdaq']


def getTimeDifferences(times):
    diffs = {}
    prev = 0
    for t in times:
        diff = t-prev
        if not diffs.get(diff):
            diffs[diff] = 1
        else:
            diffs[diff] += 1
        prev = t
    for k, v, in diffs.items():
        print(f'for {k/1000000000} seconds, there are {v} values at {getActualTime(t)}')


def getActualTime(timestamp_nano):
    d = dt.datetime.fromtimestamp(timestamp_nano/1000000000)
    print(d.strftime('%A %B %d %H:%M:%S'))


if __name__ == '__main__':

    isMarketOpen()
    # Server is on Berlin time, hmmm
    nydiff = 6
    # ticker = 'DLTR'
    tttdate = pd.Timestamp.today()
    start = dt2unix(dt.datetime.utcnow() - dt.timedelta(hours=3), 'n')
    # tdate = dt.date(start.year, start.month, start.day)
    tdate = dt.date(2021, 3, 12)
    print(start)
    # pa.cycleStocksToCurrent(['BNGO'], tdate, 0)
    # pa = PolygonApi(random50(numstocks=5), tdate, filternull=True)
    # pa = PolygonApi(nasdaq100symbols, tdate, start=start, filternull=True)
    # pa = PolygonApi(["FOX", "ILMN", "ISRG", "JD", "MXIM"], tdate, filternull=True)
    # pa.cycleStocksToCurrent()
    # # pa.cycleStocksToCurrent(['FISV'], tdate, start)
    # print()
