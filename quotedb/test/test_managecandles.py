"""

"""
# import logging
import datetime as dt
import random
from unittest import TestCase
import unittest
import pandas as pd
from quotedb.dbconnection import getSaConn
from quotedb.finnhub.finncandles import FinnCandles
from quotedb.models.candlesmodel import CandlesModel
from quotedb.models.common import createFirstQuote
from quotedb.models.managecandles import ManageCandles
from quotedb.models.metamod import init, getEngine, cleanup
from quotedb.scripts.installtestdb import installTestDb
from quotedb.sp500 import random50
from quotedb.utils.util import getPrevTuesWed, dt2unix, dt2unix_ny, unix2date_ny


def dblcheckDbMode(db=None, reverse=False):
    """In case this is called without setUpClass(), and the db is not in test mode,fail with AssertionError """
    db = db if db else getSaConn()
    if reverse:
        assert db.find("dev_stockdb") < 0
    else:
        assert db.find("dev_stockdb") > 0


class TestManageCandles(TestCase):
    stocks = None
    start = None
    fn = None

    @classmethod
    def setUpClass(cls):
        print("\nTestManageCandles.setUpClass()")
        installTestDb(install="dev")
        cleanup()
        init()
        db = getSaConn(refresh=True)
        # Prevent tests from running if not using dev_quotedb
        dblcheckDbMode(db)
        init()

        tables = ['candles', 'allquotes']
        engine = getEngine()
        for table in tables:
            statement = f"DELETE FROM {table}"
            engine.execute(statement)
        d = getPrevTuesWed(dt.datetime.now())
        cls.start = dt2unix_ny(dt.datetime(d.year, d.month, d.day, 10, 30))
        stocks = random50(numstocks=5)
        cls.stocks = stocks[:4]
        cls.extrastock = stocks[4]

        fc = FinnCandles(cls.stocks)
        fc.cycleStockCandles(cls.start, numcycles=0)

    @classmethod
    def tearDownClass(cls):
        print("TestManageCandles.tearDownClass()")
        installTestDb(install="production")
        cleanup()
        init()
        db = getSaConn(refresh=True)

        dblcheckDbMode(db, reverse=True)

    def test_createTables(self):
        """Tables are recreated in setUpClass. Test they exist"""
        print('TestManageCandles.test_createTables')
        dblcheckDbMode()
        tables = ['candles', 'allquotes']
        # Delete and restore tables
        init()
        tabnames = getEngine().table_names()
        for table in tables:
            self.assertIn(table, tabnames)

    @unittest.skip("getReport() is broken and no pressing need")
    def test_reportShape_candles(self):
        pass
        # print('TestManageCandles.test_reportShape')

    @unittest.skip("getReport() is broken and no pressing need")
    def test_chooseFromReport(self):
        pass
        # print('TestManageCandles.test_chooseFromReport')

    def test_getLargestTimeGaps(self):
        mc = ManageCandles(None, model=CandlesModel)
        stock = self.stocks[random.randint(0, len(self.stocks)-1)]
        x = mc.getLargestTimeGap(stock)
        self.assertGreater(x[0], 0)
        self.assertGreater(x[1], 0)

    def test_getFilledData(self):
        """
        Test that the results of getFilledData have the same interval between timestamps
        beginning to end
        """
        print('TestManageCandles.test_getFilledData')
        INTERVAL = 60
        cleanup()
        init()
        mc = ManageCandles(None, model=CandlesModel)
        stock = self.stocks[random.randint(0, len(self.stocks)-1)]
        begin = self.start + (60*60*3)
        end = self.start + (60*60*5)
        fmt = 'csv'

        data = mc.getFilledData(stock, begin, end, format=fmt)

        for i, t in enumerate(data.timestamp):
            if i == 0:
                oldval = t
                continue
            self.assertEqual(int(t - oldval), INTERVAL)
            oldval = t

    @unittest.skip("Lots of weirdnesses in conception and execution. Not sure if this has a use. Cutting time spent")
    def test_getFilledDataDays(self):
        print('TestManageCandles.test_getFilledDataDays')
        mc = ManageCandles(None, model=CandlesModel)
        stock = self.stocks[random.randint(0, len(self.stocks)-1)]
        maxt = unix2date_ny(mc.getMaxTime(stock, mc.session))
        mint = unix2date_ny(mc.getMinTime(stock, mc.session))
        df = mc.getFilledDataDays(stock, mint, maxt, policy="market", format="csv")
        curday = unix2date_ny(df.iloc[0].timestamp).date()
        while curday <= unix2date_ny(df.iloc[-1].timestamp).date():
            pass

    def test_getMaxTimeForEachTicker(self):
        print('TestManageCandles.test_getMaxTimeForEachTicker')
        mc = ManageCandles(None, model=CandlesModel)
        maxtimes = mc.getMaxTimeForEachTicker(self.stocks)
        self.assertEqual(len(maxtimes), len(self.stocks))

    def test_filterGainersLosers(self):
        print('TestManageCandles.test_filterGainersLosers')
        mc = ManageCandles(None, model=CandlesModel)
        gainers, losers = mc.filterGainersLosers(self.stocks, self.start, len(self.stocks))
        self.assertEqual((len(gainers[1:]) + len(losers[1:])), len(self.stocks))
        print()

    def test_addCandles(self):
        print('TestManageCandles.test_addCandles')
        fc = FinnCandles([self.extrastock])

        mc = ManageCandles(None, model=CandlesModel)
        end = dt2unix(dt.datetime.utcnow())
        candles = fc.getDateRange(fc.tickers, self.start, end)
        mc.addCandles(self.extrastock, candles, mc.session)
        candles = mc.getTimeRange(self.extrastock, self.start, end)
        self.assertIsInstance(candles[0], CandlesModel)

    def test_getTimeRange(self):
        print('TestManageCandles.test_getTimeRange')
        mc = ManageCandles(None, model=CandlesModel)
        start = self.start
        end = start + (4*60*60)
        stock = self.stocks[random.randint(0, len(self.stocks)-1)]
        results = mc.getTimeRange(stock, start, end)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)
        self.assertIsInstance(results[0], CandlesModel)
        times = sorted([x.timestamp for x in results])
        self.assertGreaterEqual(times[0], start)
        self.assertLessEqual(times[-1], end)

    def test_getTimeRangeMultiple(self):
        print('TestManageCandles.test_getTimeRangeMultiple')
        mc = ManageCandles(None, model=CandlesModel)
        start = self.start
        end = start + (60 * 60 * 2)
        ranges = mc.getTimeRangeMultiple(self.stocks, start, end)
        self.assertIsInstance(ranges, pd.DataFrame)
        self.assertGreater(len(ranges), 0)

    def test_getTimeRangeMultipleVpts(self):
        print('TestManageCandles.test_getTimeRangeMultipleVpts')
        end = dt2unix(dt.datetime.utcnow())
        mk = ManageCandles(getSaConn, CandlesModel)
        df = mk.getTimeRangeMultipleVpts(self.stocks, self.start, end)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        self.assertEqual(set(df.columns), set(['volume', 'price', 'timestamp', 'stock']))

    def test_getTimeRangePlus(self):
        print('TestManageCandles.test_getTimeRangePlus')
        mc = ManageCandles(None, model=CandlesModel)
        cleanup()
        init()

        stock = self.stocks[random.randint(0, len(self.stocks)-1)]
        start = self.start + (60*60)
        end = start + (60*60)
        ranges = mc.getTimeRangePlus(stock, start, end)
        self.assertIsInstance(ranges, list)
        self.assertGreater(len(ranges), 0)
        self.assertIsInstance(ranges[0], CandlesModel)
        times = sorted([x.timestamp for x in ranges])
        self.assertLessEqual(times[0], start)
        self.assertLessEqual(times[-1], end)
        self.assertGreaterEqual(times[-1], start)

    def test_getMaxTime(self):
        print('TestManageCandles.test_getMaxTime')
        mc = ManageCandles(None, CandlesModel)
        stock = self.stocks[random.randint(0, len(self.stocks)-1)]
        maxt = mc.getMaxTime(stock, mc.session)
        self.assertIsInstance(maxt, int)

    def test_getMinTime(self):
        print('TestManageCandles.test_getMinTime')
        mc = ManageCandles(None, CandlesModel)
        stock = self.stocks[random.randint(0, len(self.stocks)-1)]
        maxt = mc.getMinTime(stock, mc.session)
        self.assertIsInstance(maxt, int)

    def test_getTickers(self):
        print('TestManageCandles.test_getTickers')
        mc = ManageCandles(None, CandlesModel)
        stickers = mc.getTickers()
        sstocks = self.stocks
        if len(stickers) > len(self.stocks):
            sstocks.append(self.extrastock)
        self.assertEqual(set(sstocks), set(stickers))
        print()

    def test_cleanDuplicatesFromResults(self):
        """
        Get stocks from the db and test tha cleanDuplicates finds each one
        returning an empty array
        """

        print('TestManageCandles.test_cleanDuplicatesFromResults')
        stock = self.stocks[random.randint(0, len(self.stocks)-1)]

        # fc = FinnCandles(self.stocks)
        end = self.start + (60*60*4)
        # arr = fc.getDateRange(stock, self.start, end)

        mc = ManageCandles(None, CandlesModel)
        arr = mc.getTimeRange(stock, self.start, end)
        arr = [[x.close, x.high, x.low, x.open, x.timestamp, x.volume] for x in arr]
        arr = mc.cleanDuplicatesFromResults(stock, arr, mc.session)
        # [c, h, l, o, t, v]

        self.assertEqual(arr, [])
        print()

    @unittest.skip("Broken and no pressing need to fix")
    def test_getReport(self):
        print('TestManageCandles.test_getReport')

    def test_getDeltaData(self):
        print('TestManageCandles.test_getDeltaData')
        mc = ManageCandles(None, CandlesModel)
        end = self.start + (60*60*7)
        start = self.start + (60*60*2)
        fq_date = self.start + (60*60)
        cleanup()
        init()
        fq = createFirstQuote(fq_date, CandlesModel, stocks=self.stocks, usecache=False)
        data = mc.getDeltaData(self.stocks, start, end, fq)
        self.assertIsInstance(data, pd.DataFrame)
        # data = mc.getDeltaData(self.stocks, self.start, end, fq)
        print()





if __name__ == '__main__':
    unittest.main()
    # TestManageCandles.setUpClass()
    # tmc = TestManageCandles()
    # tmc.test_getDeltaData()
    # tmc.test_getTimeRangePlus()
    # tmc.test_cleanDuplicatesFromResults()
    # tmc.test_getTimeRangeMultiple()
    # tmc.test_getFilledData()
