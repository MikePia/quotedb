import csv
import datetime as dt

import pandas as pd


from quotedb.utils.util import dt2unix, unix2date, unix2date_ny, resample
from quotedb.models.metamod import getSession, init, cleanup, getEngine
from quotedb.dbconnection import getSaConn, getCsvDirectory
from quotedb.polygon.polytrade import isMarketOpen
from sqlalchemy import desc, func, distinct, text


class ManageCandles:
    """
    Explanation
    -----------
    Manage sessions and common api for various candles tables
    """
    engine = None
    session = None

    def __init__(self, db, model, create=False):
        '''
        Explanation
        ___________
        Create a manageCandles object. It can run queries for various candle table.
        Currently that includes the tables 'candles', 'allquotes', and 'firstquotes'
        :params db: a SQLalchemy connection string.
        '''
        self.db = db
        self.session = getSession()
        self.model = model
        if create:
            self.createTables()

    def createTables(self):
        init()

    def reportShape(self, tickers=None):
        """
        Could analyze differences in db holdings here, For now just print it out
        """
        d = self.getReport(self.session, tickers)

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
        q = s.query(self.model.timestamp).filter_by(stock=ticker).order_by(self.model.timestamp).all()
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
        data = self.getTimeRangePlus(stock, begin, end)
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
            tickers = self.getTickers(self.session)
        for tick in tickers:
            t = self.getMaxTime(tick, self.session)
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
        df = self.getTimeRangeMultipleVpts(tickers, start, end)
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

    def addCandles(self, stock, arr, session):
        '''
        [c, h, l, o, t, v]
        '''
        retries = 5
        while retries > 0:
            try:
                init()
                s = getSession()
                arr = self.cleanDuplicatesFromResults(stock, arr, session)
                if len(arr) == 0:
                    return
                for i, t in enumerate(arr, start=1):
                    s.add(self.model(
                        stock=stock,
                        close=t[0],
                        high=t[1],
                        low=t[2],
                        open=t[3],
                        timestamp=t[4],
                        volume=t[5]))
                    if not i % 1000:
                        s.commit()
                        print(f'commited {i} records for stock {stock}')

                print(f'commited {len(arr)} records for stock {stock}')
                s.commit()
                retries = 0
            except Exception as ex:
                print(ex, f'Retry #{retries}')
                retries -= 1
                continue
            finally:
                cleanup()

    def getTimeRange(self, stock, start, end):
        s = self.session
        q = s.query(self.model).filter_by(stock=stock).filter(self.model.timestamp >= start).filter(self.model.timestamp <= end).all()
        return q

    def getTimeRangeMultiple(self, symbols, start, end):
        """
        Query candles for all stocks that have times between start and end
        """
        s = self.session
 
        q = s.query(self.model).filter(
            self.model.timestamp >= start).filter(
            self.model.timestamp <= end).filter(
            self.model.stock.in_(symbols)).order_by(
            self.model.timestamp.asc(), self.model.stock.asc()).all()
        return q

    def getTimeRangeMultipleVpts_slow(self, symbols, start, end):
        s = self.session

        q = s.query(self.model.stock, self.model.close.label("price"), self.model.timestamp, self.model.volume).filter(
            self.model.timestamp >= start).filter(
                self.model.timestamp <= end).filter(
                self.model.stock.in_(symbols)).order_by(
                self.model.timestamp.asc(), self.model.stock.asc())
        q = q.all()
        return [r._asdict() for r in q]

    def getTimeRangeMultipleVpts(self, symbols, start, end):
        print(f'Getting candles for {len(symbols)} stocks between {unix2date_ny(start)} and {unix2date_ny(end)} NY time')
        s = self.session
        q = s.query(self.model.stock, self.model.close, self.model.timestamp, self.model.volume).filter(
            self.model.timestamp >= start).filter(self.model.timestamp <= end).all()
        df = pd.DataFrame([(d.stock, d.close, d.timestamp, d.volume) for d in q], columns=['stock', 'price', 'timestamp', 'volume'])
        df = df[df.stock.isin(symbols)]
        return df

    def getTimeRangePlus(self, stock, start, end, plus=(60*30)):
        '''
        Retrieve the timestamp range but guarantee that the first timestamp has either a current value
        or a previous value
        '''
        data = self.getTimeRange(stock, start-plus, end)
        if not data:
            return []
        if data[0].timestamp <= start:
            return data
        s = self.session
        q = s.query(self.model).filter(self.model.timestamp < start).order_by(desc(self.model.timestamp)).first()
        if q:
            data.insert(0, q)
        else:
            # TODO
            print('No current or earlier data for start')
            raise ValueError('Programmers Exception, Here is the case to deal with')
        return data

    def getMaxTime(self, ticker, session):
        s = session
        q = s.query(func.max(self.model.timestamp)).filter_by(stock=ticker).one_or_none()
        return q[0]

    def getTickers(self):
        s = self.session

        tickers = s.query(distinct(self.model.stock)).all()
        tickers = [x[0] for x in tickers]
        return tickers

    def cleanDuplicatesFromResults(self, stock, arr, session):
        """
        Remove results from arr that have a duplicate timestamp and stock in the db
        """
        s = session
        td = {t[4]: t for t in arr}
        times = set(list(td.keys()))
        q = s.query(self.model.timestamp).filter_by(stock=stock).filter(
                self.model.timestamp >= min(times)).filter(
                self.model.timestamp <= max(times)).order_by(self.model.timestamp).all()
        times2 = set([x[0] for x in q])
        for tt in (times & times2):
            del td[tt]
        if len(times) > len(td):
            print(f'Found {len(times) - len(td)} duplicates')
        return list(td.values())

    def getReport(self, session, tickers=None):
        """
        Broke it fix later returns {}
        Currently developers tool only, it's too slow. It's Query problems and maybe some SA tweaking
        But it is really useful even looking over it with eyes can see potential missing data based on
        beginning dates. It will need to be automated. 100 stocks is very different than 8000
        Get the min and max dates of tickers.
        :params tickers: list, if tickers is None, report on every ticker in the db
        :return: {ticker:[mindate<int>, maxdate<int>, numrec:<int>], ...}
        """
        # d = {}
        # s = session
        # if tickers is None:
        #     tickers = s.query(distinct(self.model.stock)).all()
        #     tickers = [x[0] for x in tickers]
        # There is probably some cool way to get all the data in one sql statement. THIS COULD BE VERY TIME CONSUMING
        # for tick in tickers[::-1]:
        #     # I think first thing is just change this to a sql execute without ORM (did not help much)
        #     # select min(timestamp), max(timestamp), count(timestamp) from candles where stock="SIRI";

        #     with engine.connect() as con:
        #         statement = text(f"""SELECT min(timestamp), max(timestamp), count(timestamp) FROM candles WHERE stock="{tick}";""")
        #         q = con.execute(statement).fetchall()
        #         if q[0][2] == 0:
        #             d[tick] = [q[0][0], q[0][1], q[0][2]]
        #             print(f'{tick}: {q[0][0]}: {q[0][1]}: {q[0][2]} ')
        #         else:
        #             d[tick] = [unix2date(q[0][0]), unix2date(q[0][1]), q[0][2]]
        #             print(f'{tick}: {unix2date(q[0][0])}: {unix2date(q[0][1])}: {q[0][2]} ')
        # return d
        return {}

    def printLatestTimes(self, stocks, session):
        from quotedb.utils.util import unix2date
        for stock in stocks:
            t = self.getMaxTime(stock, session)
            print(unix2date(t, unit='s').strftime("%A %B, %d %H:%M%S"))
        print(isMarketOpen())

    def getDeltaData(self, symbols, start, end):
        """
        Not clear on the requirements for this data yet.
        """
        data = self.getTimeMultipleVpts(symbols, start, end)
        return data

    def getFirstQuoteData(self, timestamp):

        with getEngine().connect() as con:
            statement = text(f"""
                SELECT s1.*
                    FROM allquotes s1
                        inner join  (SELECT *,  max(timestamp) as mts
                            FROM allquotes
                            WHERE timestamp <= {timestamp} GROUP BY stock) s2
                    on s2.stock = s1.stock and s1.timestamp = s2.mts """)
            q = con.execute(statement).fetchall()
        return q


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