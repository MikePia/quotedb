import csv
import datetime as dt

import pandas as pd

from quotedb.utils.util import dt2unix, unix2date, resample
from quotedb.models.candlesmodel import CandlesModel
from quotedb.models.metamod import getSession, init
from quotedb.dbconnection import getSaConn, getCsvDirectory


class ManageCandles:
    engine = None
    session = None

    def __init__(self, db, create=False):
        '''
        :params db: a SQLalchemy connection string.
        '''
        self.db = db
        self.session = getSession()
        if create:
            self.createTables()

    def createTables(self):
        init()

    def reportShape(self, tickers=None):
        """
        Could analyze differences in db holdings here, For now just print it out
        """
        d = CandlesModel.getReport(self.session, tickers)

        fn = getCsvDirectory() + "/report.csv"
        with open(fn, 'a', newline='') as file:
            for k, v, in d.items():
                csv_file = csv.writer(file)
                csv_file.writerow([k, v[0], v[1], v[2]])
                # print(f'{tick}: {unix2date(q[0][0])}: {unix2date(q[0][1])}: {q[0][2]} ')
                print(f'{k}: {v[0]}: {v[1]}: {v[2]} ')

    def chooseFromReport(self, fn, numRecords=None, minDate=None):
        csvfile = []
        with open(fn, 'r') as file:
            reader = csv.reader(file, dialect="excel")
            for row in reader:
                csvfile.append(row)

        if numRecords is not None:
            return [x[0] for x in csvfile if int(x[3]) <= numRecords]

    def getLargestTimeGap(self, ticker):
        s = getSession()
        q = s.query(CandlesModel.timestamp).filter_by(stock=ticker).order_by(CandlesModel.timestamp).all()
        maxsize = (0, 0)
        prevtime = q[0][0]
        for t in q:
            newtime = t[0]
            if newtime-prevtime > maxsize[0]:
                maxsize = (newtime-prevtime, newtime)
            prevtime = newtime
        print('max timestamp is ', dt.timedelta(seconds=maxsize[0]))
        print('Occurs at ', unix2date(maxsize[1]))

    def getFilledData(self, stock, begin, end, format='json'):
        '''
        The atomic method to Query db for data between begin and end and return one record
        for each minute. Note that this is atomic because it is intended for data on a single day. Use
        getFilledDataDays for multiday
        '''
        data = CandlesModel.getTimeRangePlus(stock, begin, end, self.session)
        if not data:
            return None
        datadict = [x.__dict__ for x in data]
        cols = ['open', 'high', 'low', 'close', 'timestamp', 'volume']
        datadict = pd.DataFrame.from_dict(datadict)[cols]
        data = resample(datadict, 'time', dt.timedelta(seconds=60))
        data.close = data.close.fillna(method='ffill')
        data.open = data.open.fillna(data.close)
        data.high = data.high.fillna(data.close)
        data.low = data.low.fillna(data.close)
        data.volume = data.volume.fillna(0)
        if format == 'csv':
            return data
        return data.to_json()

    # def getFiilledDataForMultiple

    def getFilledDataDays(self, stock, startdate, enddate, policy='extended', custom=None, format='json'):
        '''
        Parameters
        ----------
        :params startdate: dt.date : Get data beginning on this day
        :params enddate: dt.date : Get data ending on this day (inclusive)
        :params policy: str : [market, extended, 24_7]
            'market' will return data between 9:30 and 16:00
            'extended' will return data between 7:00 and 19:00
            '24_7' will return the entire day  (Actually 24-5, no weekends)
        :params custom: (begin<dt.datetime>, end<dt.datetime>) : Overrides policy to return using data between begin and end
        '''
        delt = dt.timedelta(days=1)
        current = startdate
        if custom:
            begin = custom[0]
            end = custom[1]
        else:
            if policy == 'extended':
                begin = dt.time(7, 0, 0)
                end = dt.time(19, 0, 0)
            elif policy == 'market':
                begin = dt.time(9, 30, 0)
                end = dt.time(16, 0, 0)
            elif policy == '24_7':
                begin = dt.time(0, 0, 0)
                end = dt.time(23, 59, 0)
        df = None
        while current <= enddate:
            if current.weekday() < 5:
                curstart = int(pd.Timestamp(current.year, current.month, current.day, begin.hour, begin.minute, begin.second).timestamp())
                curend = int(pd.Timestamp(current.year, current.month, current.day, end.hour, end.minute, end.second).timestamp())
                newdf = self.getFilledData(stock, curstart, curend, format='csv')
                if df is None and newdf is not None:
                    df = newdf
                elif newdf is not None:
                    df = df.append(newdf, ignore_index=True)
            current += delt
        if format == 'csv':
            return df
        return df.to_json() if df is not None else df

    def getMaxTimeForEachTicker(self, tickers=None):
        maxdict = dict()
        if tickers is None:
            tickers = CandlesModel.getTickers(self.session)
        for tick in tickers:
            t = CandlesModel.getMaxTime(tick, self.session)
            if t:
                maxdict[tick] = t
        return maxdict

    def filterGanersLosers(self, tickers, start, numstocks):
        """
        Explanation
        -----------
        filter the stocks in tickers to include the numstocks fastest gainers and losers
        Will return two arrays, one for gainers, one for losers each of length numstocks
        """
        # end is just some time in the future
        end = dt2unix(dt.datetime.utcnow() + dt.timedelta(hours=5))
        df = CandlesModel.getTimeRangeMultipleVpts(tickers, start, end, self.session)
        gainers = []
        losers = []
        for tick in df.stock.unique():
            t = df[df.stock == tick]
            t = t.copy()
            t.sort_values(['timestamp'], inplace=True)

            firstprice, lastprice = t.iloc[0].price, t.iloc[-1].price
            pricediff = firstprice - lastprice
            percentage = abs(pricediff / firstprice)
            if pricediff >= 0:
                gainers.append([tick, pricediff, percentage, firstprice, lastprice])
            else:
                losers.append([tick, pricediff, percentage, firstprice, lastprice])

        gainers.sort(key=lambda x: x[2], reverse=True)
        losers.sort(key=lambda x: x[2], reverse=True)
        gainers = gainers[:10]
        losers = losers[:10]
        gainers.insert(0, ['stock', 'pricediff', 'percentage', 'firstprice', 'lastprice'])
        losers.insert(0, ['stock', 'pricediff', 'percentage', 'firstprice', 'lastprice'])
        return gainers, losers


def getRange():
    d1 = dt.date(2021, 2, 23)
    d2 = dt.date(2021, 3, 12)
    mc = ManageCandles(getSaConn())
    stock = 'ZM'
    x = mc.getFilledDataDays(stock, d1, d2)
    return x


if __name__ == '__main__':
    # getRange()
    # mc = ManageCandles(getSaConn(), True)
    # mc = ManageCandles(getSaConn())
    # CandlesModel.printLatestTimes(nasdaq100symbols, mc.session)
    # mc.getLargestTimeGap('ZM')
    # mc.chooseFromReport(getCsvDirectory() + '/report.csv')
    # tickers = ['TXN', 'SNPS', 'SPLK', 'PTON', 'CMCSA', 'GOOGL']
    # mc.reportShape(tickers=mc.getQ100_Sp500())

    # #################################################################
    # from pprint import pprint
    # mc = ManageCandles(getSaConn())
    # start = dt2unix(pd.Timestamp(2021,  3, 16, 12, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    # print(pd.Timestamp(start, unit='s'))
    # # stocks = ['AAPL', 'SQ']
    # stocks = getQ100_Sp500()
    # gainers, losers = mc.filterGanersLosers(stocks, start, 10)
    #####################################
    # start = 1609463187
    # end = 1615943202
    # print(unix2date(start))
    # print(unix2date(end))
    # stocks = getQ100_Sp500()

    # mc = ManageCandles(getSaConn())
    # df = CandlesModel.getTimeRangeMultipleVpts(stocks, start, end, mc.session)
    #############################################
    mc = ManageCandles(getSaConn(), create=True)
    start = dt2unix(pd.Timestamp(2021,  3, 12, 12, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    end = dt2unix(pd.Timestamp.utcnow().replace(tzinfo=None))
    x = mc.getFilledData('AAPL', start, end)
